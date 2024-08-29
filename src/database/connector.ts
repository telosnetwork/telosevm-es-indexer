import RPCBroadcaster from '../publisher.js';
import {
    IndexedBlockInfo,
    TranslatorConfig,
    IndexerState,
} from '../types/translator.js';
import { getTemplatesForChain } from './templates.js';

import { Client, estypes } from '@elastic/elasticsearch';
import {
    isStorableDocument,
    StorageEosioAction,
    StorageEosioDelta,
    StorageEosioDeltaSchema,
    StorageEosioGenesisDeltaSchema,
} from '../types/evm.js';
import { createLogger, format, Logger } from 'winston';
import EventEmitter from 'events';

interface ConfigInterface {
    [key: string]: any;
}

function indexToSuffixNum(index: string) {
    const spltIndex = index.split('-');
    const suffix = spltIndex[spltIndex.length - 1];
    return parseInt(suffix);
}

// export class ElasticScroller {
//     elastic: Client;
//     scrollId: string;
//     done: boolean = false;
//     readonly initialDocs: any[];
//
//     constructor(elastic: Client, scrollResponse: SearchResponse) {
//         this.elastic = elastic;
//         this.initialDocs = scrollResponse.hits.hits.map(h => h._source);
//         this.scrollId = scrollResponse._scroll_id;
//     }
//
//     async nextScroll() {
//         const scrollResponse = await this.elastic.scroll({
//             scroll_id: this.scrollId,
//             scroll: '1s'
//         });
//
//         const hits = scrollResponse.hits.hits;
//
//         if (hits.length === 0) {
//             this.done = true;
//             return [];
//         }
//
//         return hits.map(hit => hit._source);
//     }
//
//     async *[Symbol.asyncIterator]() {
//         yield this.initialDocs;
//
//         while (!this.done) {
//             const hits = await this.nextScroll();
//             if (hits.length > 0) yield hits;
//         }
//     }
// }

export interface ScrollOptions {
    fields?: string[];
    scroll?: string;
    size?: number;
}

export class BlockScroller {
    private readonly conn: Connector;
    private readonly from: number;
    private readonly to: number;
    private readonly validate: boolean;
    private readonly scrollOpts: ScrollOptions;

    private last: number;
    private currentSuffNum: number;
    private currentDeltaScrollId: string;
    private endSuffNum: number;

    private deltaIndices = new Map<number, string>();
    private done: boolean = false;

    private initialHits: StorageEosioDelta[];

    constructor(
        connector: Connector,
        params: {
            from: number;
            to: number;
            validate?: boolean;
            scrollOpts?: ScrollOptions;
        }
    ) {
        this.conn = connector;
        this.from = params.from;
        this.to = params.to;
        this.scrollOpts = params.scrollOpts ? params.scrollOpts : {};
        this.validate = params.validate ? params.validate : false;
    }

    private unwrapDeltaHit(hit): StorageEosioDelta {
        const doc = hit._source;
        if (this.validate) return StorageEosioGenesisDeltaSchema.parse(doc);

        return doc;
    }

    /*
     * Perform initial scroll/search request on current index
     */
    private async scrollRequest(): Promise<{
        scrollId: string;
        hits: StorageEosioDelta[];
    }> {
        const deltaResponse = await this.conn.elastic.search({
            index: this.deltaIndices.get(this.currentSuffNum),
            scroll: this.scrollOpts.scroll ? this.scrollOpts.scroll : '1s',
            size: this.scrollOpts.size ? this.scrollOpts.size : 1000,
            sort: [{ block_num: 'asc' }],
            query: {
                range: {
                    block_num: {
                        gte: this.from,
                        lte: this.to,
                    },
                },
            },
            _source: this.scrollOpts.fields,
        });
        const deltaScrollInfo = {
            scrollId: deltaResponse._scroll_id,
            hits: deltaResponse.hits.hits.map((h) => this.unwrapDeltaHit(h)),
        };
        return deltaScrollInfo;
    }

    /*
     * Scroll an already open request, if we got 0 hits
     * check if reached end, if not try to move to next delta index
     */
    private async nextScroll(): Promise<StorageEosioDelta[]> {
        let blockDocs = [];

        try {
            const deltaScrollResponse = await this.conn.elastic.scroll({
                scroll_id: this.currentDeltaScrollId,
                scroll: this.scrollOpts.scroll ? this.scrollOpts.scroll : '1s',
            });
            let hits = deltaScrollResponse.hits.hits;

            if (hits.length === 0) {
                // clear current scroll context
                await this.conn.elastic.clearScroll({
                    scroll_id: this.currentDeltaScrollId,
                });
                // is scroll done?
                if (this.last == this.to) this.done = true;
                else {
                    this.currentSuffNum++;
                    // are indexes exhausted?
                    if (this.currentSuffNum > this.endSuffNum)
                        throw new Error(
                            `Scanned all relevant indexes but didnt reach ${this.to}`
                        );

                    // open new scroll & return hits from next one
                    const scrollInfo = await this.scrollRequest();
                    this.currentDeltaScrollId = scrollInfo.scrollId;
                    blockDocs = scrollInfo.hits;
                }
            } else blockDocs = hits.map((h) => this.unwrapDeltaHit(h));

            if (blockDocs.length > 0) {
                // @ts-ignore
                this.last = blockDocs[blockDocs.length - 1].block_num;
            }
        } catch (e) {
            this.conn.logger.error(
                'BlockScroller error while fetching next batch:'
            );
            this.conn.logger.error(e.message);
            throw e;
        }

        return blockDocs;
    }

    /*
     * Setup index map with index names necessary for full scroll,
     * then perform first scroll request and seed initialHits property
     */
    async init() {
        // get relevant indexes
        this.currentSuffNum = parseInt(this.conn.getSuffixForBlock(this.from));
        this.endSuffNum = parseInt(this.conn.getSuffixForBlock(this.to));
        const relevantIndexes = await this.conn.getRelevantDeltaIndicesForRange(
            this.from,
            this.to
        );
        relevantIndexes.forEach((index) => {
            const indexSuffNum = indexToSuffixNum(index);
            if (
                indexSuffNum >= this.currentSuffNum &&
                indexSuffNum <= this.endSuffNum
            )
                this.deltaIndices.set(indexSuffNum, index);
        });
        if (this.deltaIndices.size == 0)
            throw new Error(`Could not find any delta indices to scroll`);

        // first scroll request
        const scrollInfo = await this.scrollRequest();
        this.initialHits = scrollInfo.hits;
        this.currentDeltaScrollId = scrollInfo.scrollId;

        // bail early if no hits
        if (this.initialHits.length > 0)
            this.last = this.initialHits[this.initialHits.length - 1].block_num;
        else this.done = true;
    }

    /*
     * Important before using this in a for..of statement,
     * call this.init! class gets info about indexes needed
     * for scroll from elastic
     */
    async *[Symbol.asyncIterator](): AsyncIterableIterator<
        StorageEosioDelta[]
    > {
        yield this.initialHits;

        while (!this.done) {
            const hits = await this.nextScroll();
            if (hits.length > 0) yield hits;
        }
    }
}

export class Connector {
    config: TranslatorConfig;
    logger: Logger;
    elastic: Client;
    chainName: string;

    state: IndexerState;

    blockDrain: IndexedBlockInfo[];
    opDrain: any[];

    totalPushed: number = 0;
    lastPushed: number = 0;

    writeCounter: number = 0;
    lastDeltaIndexSuff: number = undefined;
    lastActionIndexSuff: number = undefined;

    broadcast: RPCBroadcaster;
    isBroadcasting: boolean = false;

    events = new EventEmitter();

    constructor(config: TranslatorConfig, logger?: Logger) {
        this.config = config;
        if (logger) this.logger = logger;
        else {
            const loggingOptions = {
                exitOnError: false,
                level: 'error',
            };
            this.logger = createLogger(loggingOptions);
        }
        this.chainName = config.chainName;
        this.elastic = new Client(config.elastic);

        this.broadcast = new RPCBroadcaster(config.broadcast, logger);

        this.opDrain = [];
        this.blockDrain = [];

        this.state = IndexerState.SYNC;
    }

    getSuffixForBlock(blockNum: number) {
        const adjustedNum = Math.floor(
            blockNum / this.config.elastic.docsPerIndex
        );
        return String(adjustedNum).padStart(8, '0');
    }

    async init() {
        const indexConfig: ConfigInterface = getTemplatesForChain(
            this.chainName,
            this.config.elastic.suffix,
            this.config.elastic.numberOfShards,
            this.config.elastic.numberOfReplicas,
            this.config.elastic.refreshInterval,
            this.config.elastic.codec
        );

        this.logger.info(`Updating index templates for ${this.chainName}...`);
        let updateCounter = 0;
        for (const [name, template] of Object.entries(indexConfig)) {
            try {
                const creation_status: estypes.IndicesPutTemplateResponse =
                    await this.elastic.indices.putTemplate({
                        name: `${this.chainName}-${name}`,
                        body: template,
                    });
                if (!creation_status || !creation_status['acknowledged']) {
                    this.logger.error(
                        `Failed to create template: ${this.chainName}-${name}`
                    );
                } else {
                    updateCounter++;
                    this.logger.info(
                        `${this.chainName}-${name} template updated!`
                    );
                }
            } catch (e) {
                this.logger.error(`[FATAL] ${e.message}`);
                if (e.meta) {
                    this.logger.error(e.meta.body);
                }
                process.exit(1);
            }
        }
        this.logger.info(`${updateCounter} index templates updated`);
    }

    startBroadcast() {
        this.broadcast.initUWS();
        this.isBroadcasting = true;
    }

    stopBroadcast() {
        this.broadcast.close();
        this.isBroadcasting = false;
    }

    async deinit() {
        await this.flush();

        if (this.isBroadcasting) this.stopBroadcast();

        await this.elastic.close();
    }

    async getOrderedDeltaIndices() {
        // if (this.deltaIndexCache) return this.deltaIndexCache;

        const deltaIndices: estypes.CatIndicesResponse =
            await this.elastic.cat.indices({
                index: `${this.chainName}-${this.config.elastic.suffix.delta}-*`,
                format: 'json',
            });
        deltaIndices.sort((a, b) => {
            const aNum = indexToSuffixNum(a.index);
            const bNum = indexToSuffixNum(b.index);
            if (aNum < bNum) return -1;
            if (aNum > bNum) return 1;
            return 0;
        });

        // this.deltaIndexCache = deltaIndices;

        return deltaIndices;
    }

    async getRelevantDeltaIndicesForRange(
        from: number,
        to: number
    ): Promise<string[]> {
        const startSuffNum = Math.floor(
            from / this.config.elastic.docsPerIndex
        );
        const endSuffNum = Math.floor(to / this.config.elastic.docsPerIndex);
        return (await this.getOrderedDeltaIndices())
            .filter((index) => {
                const indexSuffNum = indexToSuffixNum(index.index);
                return (
                    indexSuffNum >= startSuffNum && indexSuffNum <= endSuffNum
                );
            })
            .map((index) => index.index);
    }

    async getOrderedActionIndices() {
        // if (this.actionIndexCache) return this.actionIndexCache;

        const actionIndices: estypes.CatIndicesResponse =
            await this.elastic.cat.indices({
                index: `${this.chainName}-${this.config.elastic.suffix.transaction}-*`,
                format: 'json',
            });
        actionIndices.sort((a, b) => {
            const aNum = indexToSuffixNum(a.index);
            const bNum = indexToSuffixNum(b.index);
            if (aNum < bNum) return -1;
            if (aNum > bNum) return 1;
            return 0;
        });

        // this.actionIndexCache = actionIndices;

        return actionIndices;
    }

    async getRelevantActionIndicesForRange(
        from: number,
        to: number
    ): Promise<string[]> {
        const startSuffNum = Math.floor(
            from / this.config.elastic.docsPerIndex
        );
        const endSuffNum = Math.floor(to / this.config.elastic.docsPerIndex);
        return (await this.getOrderedActionIndices())
            .filter((index) => {
                const indexSuffNum = indexToSuffixNum(index.index);
                return (
                    indexSuffNum >= startSuffNum && indexSuffNum <= endSuffNum
                );
            })
            .map((index) => index.index);
    }

    private unwrapSingleElasticResult(result) {
        const hits = result?.hits?.hits;

        if (!hits) throw new Error(`Elastic unwrap error hits undefined`);

        if (hits.length != 1)
            throw new Error(
                `Elastic unwrap error expected one and got ${hits.length}`
            );

        const document = hits[0]._source;

        this.logger.debug(
            `elastic unwrap document:\n${JSON.stringify(document, null, 4)}`
        );

        let parseResult = StorageEosioDeltaSchema.safeParse(document);
        if (parseResult.success) return parseResult.data;

        parseResult = StorageEosioGenesisDeltaSchema.safeParse(document);
        if (parseResult.success) return parseResult.data;

        throw new Error(`Document is not a valid StorageEosioDelta!`);
    }

    async getIndexedBlock(blockNum: number): Promise<StorageEosioDelta> {
        const suffix = this.getSuffixForBlock(blockNum);
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.suffix.delta}-${suffix}`,
                query: {
                    match: {
                        block_num: {
                            query: blockNum,
                        },
                    },
                },
            });

            return this.unwrapSingleElasticResult(result);
        } catch (error) {
            return null;
        }
    }

    async getFirstIndexedBlock(): Promise<StorageEosioDelta> {
        const indices = await this.getOrderedDeltaIndices();

        if (indices.length == 0) return null;

        const firstIndex = indices.shift().index;
        try {
            const result = await this.elastic.search({
                index: firstIndex,
                size: 1,
                sort: [{ block_num: { order: 'asc' } }],
            });

            return this.unwrapSingleElasticResult(result);
        } catch (error) {
            return null;
        }
    }

    async getLastIndexedBlock(): Promise<StorageEosioDelta> {
        const indices = await this.getOrderedDeltaIndices();
        if (indices.length == 0) return null;

        for (let i = indices.length - 1; i >= 0; i--) {
            const lastIndex = indices[i].index;
            try {
                const result = await this.elastic.search({
                    index: lastIndex,
                    size: 1,
                    sort: [{ block_num: { order: 'desc' } }],
                });
                if (result?.hits?.hits?.length == 0) continue;

                return this.unwrapSingleElasticResult(result);
            } catch (error) {
                this.logger.error(error);
                throw error;
            }
        }

        return null;
    }

    async getTransactionsForRange(
        from: number,
        to: number
    ): Promise<StorageEosioAction[]> {
        const actionIndex = await this.getRelevantActionIndicesForRange(
            from,
            to
        );
        const hits = [];
        try {
            let result = await this.elastic.search({
                index: actionIndex,
                query: {
                    range: {
                        '@raw.block': {
                            gte: from - this.config.evmBlockDelta,
                            lte: to - this.config.evmBlockDelta,
                        },
                    },
                },
                size: 4000,
                sort: [{ '@raw.block': 'asc' }, { '@raw.trx_index': 'asc' }],
                scroll: '1s',
            });

            let scrollId = result._scroll_id;
            while (result.hits.hits.length) {
                result.hits.hits.forEach((hit) => hits.push(hit._source));
                result = await this.elastic.scroll({
                    scroll_id: scrollId,
                    scroll: '1s',
                });
                scrollId = result._scroll_id;
            }
            if (hits.length)
                await this.elastic.clearScroll({ scroll_id: scrollId });
        } catch (e) {
            this.logger.error(
                `connector: elastic error when getting transactions in range:`
            );
            this.logger.error(e.message);
            throw e;
        }

        return hits;
    }

    async findGapInIndices() {
        const deltaIndices = await this.getOrderedDeltaIndices();
        this.logger.debug('delta indices: ');
        this.logger.debug(JSON.stringify(deltaIndices, null, 4));
        for (let i = 1; i < deltaIndices.length; i++) {
            const previousIndexSuffixNum = indexToSuffixNum(
                deltaIndices[i - 1].index
            );
            const currentIndexSuffixNum = indexToSuffixNum(
                deltaIndices[i].index
            );

            if (currentIndexSuffixNum - previousIndexSuffixNum > 1) {
                return {
                    gapStart: previousIndexSuffixNum,
                    gapEnd: currentIndexSuffixNum,
                };
            }
        }

        // Return null if no gaps found
        return null;
    }

    async runHistogramGapCheck(
        lower: number,
        upper: number,
        interval: number
    ): Promise<any> {
        const results = await this.elastic.search<any, any>({
            index: `${this.config.chainName}-${this.config.elastic.suffix.delta}-*`,
            size: 0,
            body: {
                query: {
                    range: {
                        '@global.block_num': {
                            gte: lower,
                            lte: upper,
                        },
                    },
                },
                aggs: {
                    block_histogram: {
                        histogram: {
                            field: '@global.block_num',
                            interval: interval,
                            min_doc_count: 0,
                        },
                        aggs: {
                            min_block: {
                                min: {
                                    field: '@global.block_num',
                                },
                            },
                            max_block: {
                                max: {
                                    field: '@global.block_num',
                                },
                            },
                        },
                    },
                },
            },
        });

        const buckets = results.aggregations.block_histogram.buckets;

        this.logger.debug(
            `runHistogramGapCheck: ${lower}-${upper}, interval: ${interval}`
        );
        this.logger.debug(JSON.stringify(buckets, null, 4));

        return buckets;
    }

    async findDuplicateDeltas(lower: number, upper: number): Promise<number[]> {
        const results = await this.elastic.search<any, any>({
            index: `${this.config.chainName}-${this.config.elastic.suffix.delta}-*`,
            size: 0,
            body: {
                query: {
                    range: {
                        '@global.block_num': {
                            gte: lower,
                            lte: upper,
                        },
                    },
                },
                aggs: {
                    duplicate_blocks: {
                        terms: {
                            field: '@global.block_num',
                            min_doc_count: 2,
                            size: 100,
                        },
                    },
                },
            },
        });

        if (results.aggregations) {
            const buckets = results.aggregations.duplicate_blocks.buckets;

            this.logger.debug(`findDuplicateDeltas: ${lower}-${upper}`);

            return buckets.map((bucket) => bucket.key); // Return the block numbers with duplicates
        } else {
            return [];
        }
    }

    async findDuplicateActions(
        lower: number,
        upper: number
    ): Promise<number[]> {
        const results = await this.elastic.search<any, any>({
            index: `${this.config.chainName}-${this.config.elastic.suffix.transaction}-*`,
            size: 0,
            body: {
                query: {
                    range: {
                        '@raw.block': {
                            gte: lower,
                            lte: upper,
                        },
                    },
                },
                aggs: {
                    duplicate_txs: {
                        terms: {
                            field: '@raw.hash',
                            min_doc_count: 2,
                            size: 100,
                        },
                    },
                },
            },
        });

        if (results.aggregations) {
            const buckets = results.aggregations.duplicate_txs.buckets;

            this.logger.debug(`findDuplicateActions: ${lower}-${upper}`);

            return buckets.map((bucket) => bucket.key); // Return the block numbers with duplicates
        } else {
            return [];
        }
    }

    async checkGaps(
        lowerBound: number,
        upperBound: number,
        interval: number
    ): Promise<number> {
        interval = Math.ceil(interval);

        // Base case
        if (interval == 1) {
            return lowerBound;
        }

        const middle = Math.floor((upperBound + lowerBound) / 2);

        this.logger.debug(`calculated middle ${middle}`);

        this.logger.debug('first half');
        // Recurse on the first half
        const lowerBuckets = await this.runHistogramGapCheck(
            lowerBound,
            middle,
            interval / 2
        );
        if (lowerBuckets.length === 0) {
            return middle; // Gap detected
        } else if (
            lowerBuckets[lowerBuckets.length - 1].max_block.value < middle
        ) {
            const lowerGap = await this.checkGaps(
                lowerBound,
                middle,
                interval / 2
            );
            if (lowerGap) return lowerGap;
        }

        this.logger.debug('second half');
        // Recurse on the second half
        const upperBuckets = await this.runHistogramGapCheck(
            middle + 1,
            upperBound,
            interval / 2
        );
        if (upperBuckets.length === 0) {
            return middle + 1; // Gap detected
        } else if (upperBuckets[0].min_block.value > middle + 1) {
            const upperGap = await this.checkGaps(
                middle + 1,
                upperBound,
                interval / 2
            );
            if (upperGap) return upperGap;
        }

        // Check for gap between the halves
        if (
            lowerBuckets[lowerBuckets.length - 1].max_block.value + 1 <
            upperBuckets[0].min_block.value
        ) {
            return lowerBuckets[lowerBuckets.length - 1].max_block.value;
        }

        // Find gaps inside bucket by doc_count
        const buckets = [...lowerBuckets, ...upperBuckets];
        for (let i = 0; i < buckets.length; i++) {
            if (
                buckets[i].doc_count !=
                buckets[i].max_block.value - buckets[i].min_block.value + 1
            ) {
                const insideGap = await this.checkGaps(
                    buckets[i].min_block.value,
                    buckets[i].max_block.value,
                    interval / 2
                );
                if (insideGap) return insideGap;
            }
        }

        // No gap found
        return null;
    }

    async fullIntegrityCheck(): Promise<number> {
        const lowerBoundDoc = await this.getFirstIndexedBlock();
        const upperBoundDoc = await this.getLastIndexedBlock();

        if (!lowerBoundDoc || !upperBoundDoc) {
            return null;
        }

        const lowerBound = lowerBoundDoc['@global'].block_num;
        const upperBound = upperBoundDoc['@global'].block_num;

        const lowerBoundDelta = lowerBoundDoc.block_num - lowerBound;
        if (lowerBoundDelta != this.config.evmBlockDelta) {
            this.logger.error(
                `wrong block delta on lower bound doc ${lowerBoundDelta}`
            );
            throw new Error(
                `wrong block delta on lower bound doc ${lowerBoundDelta}`
            );
        }

        const upperBoundDelta = upperBoundDoc.block_num - upperBound;
        if (upperBoundDelta != this.config.evmBlockDelta) {
            this.logger.error(
                `wrong block delta on upper bound doc ${upperBoundDelta}`
            );
            throw new Error(
                `wrong block delta on upper bound doc ${upperBoundDelta}`
            );
        }

        const step = 10000000; // 10 million blocks

        const deltaDups = [];
        const actionDups = [];

        for (
            let currentLower = lowerBound;
            currentLower < upperBound;
            currentLower += step
        ) {
            const currentUpper = Math.min(currentLower + step, upperBound);

            // check duplicates for the current range
            deltaDups.push(
                ...(await this.findDuplicateDeltas(currentLower, currentUpper))
            );
            actionDups.push(
                ...(await this.findDuplicateActions(currentLower, currentUpper))
            );

            this.logger.info(
                `checked range ${currentLower}-${currentUpper} for duplicates, found: ${deltaDups.length + actionDups.length}`
            );
        }

        if (deltaDups.length > 0)
            this.logger.error(
                `block duplicates found: ${JSON.stringify(deltaDups)}`
            );

        if (actionDups.length > 0)
            this.logger.error(
                `tx duplicates found: ${JSON.stringify(actionDups)}`
            );

        if (deltaDups.length + actionDups.length > 0)
            throw new Error('Duplicates found!');

        if (upperBound - lowerBound < 2) return null;

        // first just check if whole indices are missing
        const gap = await this.findGapInIndices();
        if (gap) {
            this.logger.debug(`whole index seems to be missing `);
            const lower = gap.gapStart * this.config.elastic.docsPerIndex;
            const upper = (gap.gapStart + 1) * this.config.elastic.docsPerIndex;
            const agg = await this.runHistogramGapCheck(
                lower,
                upper,
                this.config.elastic.docsPerIndex
            );
            return agg[0].max_block.value;
        }

        const initialInterval = upperBound - lowerBound;

        this.logger.info(
            `starting full gap check from ${lowerBound} to ${upperBound}`
        );

        return this.checkGaps(lowerBound, upperBound, initialInterval);
    }

    async _deleteBlocksInRange(startBlock: number, endBlock: number) {
        const targetSuffix = this.getSuffixForBlock(endBlock);
        const deltaIndex = `${this.chainName}-${this.config.elastic.suffix.delta}-${targetSuffix}`;
        const actionIndex = `${this.chainName}-${this.config.elastic.suffix.transaction}-${targetSuffix}`;

        try {
            await this._deleteFromIndex(
                deltaIndex,
                'block_num',
                startBlock,
                endBlock
            );
            await this._deleteFromIndex(
                actionIndex,
                '@raw.block',
                startBlock - this.config.evmBlockDelta,
                endBlock - this.config.evmBlockDelta
            );
        } catch (e) {
            if (
                e.name != 'ResponseError' ||
                e.meta.body.error.type != 'index_not_found_exception'
            ) {
                throw e;
            }
        }
    }

    async _deleteFromIndex(
        index: string,
        blockField: string,
        startBlock: number,
        endBlock: number
    ) {
        const result = await this.elastic.deleteByQuery({
            index: index,
            body: {
                query: {
                    range: {
                        [blockField]: {
                            gte: startBlock,
                            lte: endBlock,
                        },
                    },
                },
            },
            conflicts: 'proceed',
            refresh: true,
            error_trace: true,
        });
        this.logger.debug(
            `${index} delete result: ${JSON.stringify(result, null, 4)}`
        );
    }

    async _purgeBlocksNewerThan(blockNum: number) {
        const lastBlock = await this.getLastIndexedBlock();
        if (lastBlock == null) return;
        const maxBlockNum = lastBlock.block_num;
        const batchSize = Math.min(maxBlockNum, 20000); // Batch size set to 20,000
        const maxConcurrentDeletions = 4; // Maximum of 4 deletions in parallel

        for (
            let startBlock = blockNum;
            startBlock <= maxBlockNum;
            startBlock += batchSize * maxConcurrentDeletions
        ) {
            const deletionTasks = [];

            for (
                let i = 0;
                i < maxConcurrentDeletions &&
                startBlock + i * batchSize <= maxBlockNum;
                i++
            ) {
                const batchStart = startBlock + i * batchSize;
                const batchEnd = Math.min(
                    batchStart + batchSize - 1,
                    maxBlockNum
                );
                deletionTasks.push(
                    this._deleteBlocksInRange(batchStart, batchEnd)
                );
            }

            await Promise.all(deletionTasks);
            const batchEnd = Math.min(
                startBlock + batchSize * maxConcurrentDeletions,
                maxBlockNum
            );
            this.logger.info(
                `deleted blocks from ${startBlock} to ${batchEnd}`
            );
        }
    }

    async _purgeIndicesNewerThan(blockNum: number) {
        this.logger.info(`purging indices in db from block ${blockNum}...`);
        const targetSuffix = this.getSuffixForBlock(blockNum);
        const targetNum = parseInt(targetSuffix);

        const deleteList = [];

        const deltaIndices = await this.elastic.cat.indices({
            index: `${this.config.chainName}-${this.config.elastic.suffix.delta}-*`,
            format: 'json',
        });

        for (const deltaIndex of deltaIndices)
            if (indexToSuffixNum(deltaIndex.index) > targetNum)
                deleteList.push(deltaIndex.index);

        const actionIndices = await this.elastic.cat.indices({
            index: `${this.config.chainName}-${this.config.elastic.suffix.transaction}-*`,
            format: 'json',
        });

        for (const actionIndex of actionIndices)
            if (indexToSuffixNum(actionIndex.index) > targetNum)
                deleteList.push(actionIndex.index);

        if (deleteList.length > 0) {
            const deleteResult = await this.elastic.indices.delete({
                index: deleteList,
            });
            this.logger.info(
                `deleted indices result: ${JSON.stringify(deleteResult, null, 4)}`
            );
        }

        return deleteList;
    }

    async purgeNewerThan(blockNum: number) {
        await this._purgeIndicesNewerThan(blockNum);
        await this._purgeBlocksNewerThan(blockNum);
    }

    async flush() {
        if (this.opDrain.length == 0) return;
        this.writeCounter++;
        await this.writeBlocks();
    }

    async pushBlock(blockInfo: IndexedBlockInfo) {
        const currentBlock = blockInfo.delta.block_num;
        if (this.totalPushed != 0 && currentBlock != this.lastPushed + 1)
            throw new Error(
                `Expected: ${this.lastPushed + 1} and got ${currentBlock}`
            );

        const suffix = this.getSuffixForBlock(blockInfo.delta.block_num);
        const txIndex = `${this.chainName}-${this.config.elastic.suffix.transaction}-${suffix}`;
        const dtIndex = `${this.chainName}-${this.config.elastic.suffix.delta}-${suffix}`;

        const txOperations = blockInfo.transactions.flatMap((doc) => [
            {
                create: {
                    _index: txIndex,
                    _id: `${this.chainName}-tx-${currentBlock}-${doc['@raw'].trx_index}`,
                },
            },
            doc,
        ]);

        // const operations = [
        //     ...txOperations,
        //     {create: {_index: dtIndex, _id: `${this.chainName}-block-${currentBlock}`}}, blockInfo.delta
        // ];
        const operations = [];
        Array.prototype.push.apply(operations, txOperations);
        operations.push(
            {
                create: {
                    _index: dtIndex,
                    _id: `${this.chainName}-block-${currentBlock}`,
                },
            },
            blockInfo.delta
        );

        // this.opDrain = [...this.opDrain, ...operations];
        Array.prototype.push.apply(this.opDrain, operations);
        this.blockDrain.push(blockInfo);

        this.lastPushed = currentBlock;
        this.totalPushed++;

        if (
            this.state == IndexerState.HEAD ||
            this.opDrain.length >= this.config.perf.elasticDumpSize * 2
        ) {
            await this.flush();
        }
    }

    forkCleanup(
        timestamp: string,
        lastNonForked: number,
        lastForked: number,
        diagnostics: any
    ) {
        // fix state flag
        this.lastPushed = lastNonForked;

        // clear elastic operations drain
        let i = this.opDrain.length - 1;
        while (i > 0) {
            const op = this.opDrain[i];
            if (op && isStorableDocument(op) && op.block_num > lastNonForked) {
                this.opDrain.splice(i - 1); // delete indexing info
                this.opDrain.splice(i); // delete the document
            }
            i -= 2;
        }

        // clear broadcast queue
        i = this.blockDrain.length;
        while (i > 0) {
            const block = this.blockDrain[i];
            if (block && block.delta.block_num > lastNonForked) {
                this.blockDrain.splice(i);
            }
            i--;
        }

        // write information about fork event
        const suffix = this.getSuffixForBlock(lastNonForked);
        const frkIndex = `${this.chainName}-${this.config.elastic.suffix.fork}-${suffix}`;
        this.opDrain.push({ index: { _index: frkIndex } });
        this.opDrain.push({
            timestamp,
            lastNonForked,
            lastForked,
            diagnostics,
        });
    }

    async writeBlocks() {
        const bulkResponse = await this.elastic.bulk({
            refresh: true,
            operations: this.opDrain,
            error_trace: true,
        });

        const first = this.blockDrain[0].delta.block_num;
        const last =
            this.blockDrain[this.blockDrain.length - 1].delta.block_num;

        const lastAdjusted = Math.floor(
            last / this.config.elastic.docsPerIndex
        );

        if (lastAdjusted !== this.lastDeltaIndexSuff) {
            this.lastDeltaIndexSuff = lastAdjusted;
        }

        if (lastAdjusted !== this.lastActionIndexSuff) {
            this.lastActionIndexSuff = lastAdjusted;
        }

        if (bulkResponse.errors) {
            const erroredDocuments: any[] = [];
            // The items array has the same order of the dataset we just indexed.
            // The presence of the `error` key indicates that the operation
            // that we did for the document has failed.
            bulkResponse.items.forEach(
                (
                    action: Partial<
                        Record<
                            estypes.BulkOperationType,
                            estypes.BulkResponseItem
                        >
                    >,
                    i: number
                ) => {
                    const operation = Object.keys(action)[0];
                    // @ts-ignore
                    if (action[operation].error) {
                        erroredDocuments.push({
                            // If the status is 429 it means that you can retry the document,
                            // otherwise it's very likely a mapping error, and you should
                            // fix the document before to try it again.
                            // @ts-ignore
                            status: action[operation].status,
                            // @ts-ignore
                            error: action[operation].error,
                            operation: this.opDrain[i * 2],
                            document: this.opDrain[i * 2 + 1],
                        });
                    }
                }
            );

            throw new Error(JSON.stringify(erroredDocuments, null, 4));
        }
        this.logger.info(`drained ${this.opDrain.length} operations.`);
        if (this.isBroadcasting) {
            this.logger.info(
                `broadcasting ${this.blockDrain.length} blocks...`
            );

            for (const block of this.blockDrain)
                this.broadcast.broadcastBlock(block);
        }

        this.logger.info('done.');

        this.opDrain = [];
        this.blockDrain = [];

        this.writeCounter--;

        this.events.emit('write', {
            from: first,
            to: last,
        });

        // if (global.gc) {global.gc();}
    }

    async dumpAll(
        from: number,
        to: number
    ): Promise<{ blocks: StorageEosioDelta[]; actions: StorageEosioAction[] }> {
        const blocks = [];
        const scroller = await this.blockScroll({ from, to });
        for await (const scrollResult of scroller) {
            blocks.push(...scrollResult);
        }

        const actions = await this.getTransactionsForRange(from, to);

        return { blocks, actions };
    }

    async blockScroll(params: {
        from: number;
        to: number;
        validate?: boolean;
        scrollOpts?: ScrollOptions;
    }) {
        const scroller = new BlockScroller(this, params);
        await scroller.init();
        return scroller;
    }
}

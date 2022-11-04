import StateHistoryBlockReader from './ship';


import {
    IndexedBlockInfo,
    IndexerConfig,
    IndexerState,
    StartBlockInfo
} from './types/indexer';

import logger from './utils/winston';

import {StorageEosioAction, StorageEosioDelta} from './types/evm';

import {Connector} from './database/connector';

import {
    BlockHeader,
    generateBloom,
    generateReceiptRootHash,
    generateTxRootHash,
    getBlockGasUsed,
    ProcessedBlock
} from './utils/evm'

import moment from 'moment';
const PriorityQueue = require("js-priority-queue");

const BN = require('bn.js');

const sleep = (ms: number) => new Promise( res => setTimeout(res, ms));



export class TEVMIndexer {
    endpoint: string;
    wsEndpoint: string;

    evmDeployBlock: number;
    startBlock: number;
    stopBlock: number;
    ethGenesisHash: string;

    state: IndexerState = IndexerState.SYNC;
    switchingState: boolean = false;

    config: IndexerConfig;

    private reader: StateHistoryBlockReader;
    connector: Connector;

    private prevHash: string;
    lastOrderedBlock: number;
    lastNativeOrderedBlock: number;
    private blocksQueue: typeof PriorityQueue = new PriorityQueue({
        comparator: function(a: ProcessedBlock, b: ProcessedBlock) {
            return a.evmBlockNumber - b.evmBlockNumber;
        }
    });
    // private blocksQueue: Map<number, ProcessedBlock> = new Map();
    private ordering: boolean = false;

    // debug status
    private queuedUpLastSecond: number = 0;
    private pushedLastSecond: number = 0;
    private idleWorkers: number = 0;

    constructor(telosConfig: IndexerConfig) {
        this.config = telosConfig;

        this.endpoint = telosConfig.endpoint;
        this.wsEndpoint = telosConfig.wsEndpoint;

        this.evmDeployBlock = telosConfig.evmDeployBlock;

        this.startBlock = telosConfig.startBlock;
        this.stopBlock = telosConfig.stopBlock;

        this.connector = new Connector(telosConfig);

        this.reader = new StateHistoryBlockReader(this);
        this.reader.setOptions({
            min_block_confirmation: 0,
            ds_threads: telosConfig.perf.workerAmount,
            allow_empty_deltas: true,
            allow_empty_traces: true,
            allow_empty_blocks: true
        });
    }

    updateDebugStats() {
        logger.debug(`Last second ${this.queuedUpLastSecond} blocks were queued up.`);
        let statsString = `${this.lastOrderedBlock} pushed, at ${this.pushedLastSecond} blocks/sec` +
            ` ${this.idleWorkers}/${this.config.perf.concurrencyAmount} workers idle`;
        const untilHead = this.reader.headBlock - this.reader.currentBlock;

        if (untilHead > 3) {
            const hoursETA = `${((untilHead / this.pushedLastSecond) / (60 * 60)).toFixed(1)}hs`;
            statsString += `, ${untilHead} to reach head, aprox ${hoursETA}`;
        }

        logger.info(statsString);
        this.queuedUpLastSecond = 0;
        this.pushedLastSecond = 0;
    }

    orderer() {
        // make sure we have blocks we need to order, no other orderer task
        // is running
        if (this.ordering || this.blocksQueue.length == 0)
            return;

        this.ordering = true;

        logger.debug('Running orderer...');
        let newestBlock: ProcessedBlock = this.blocksQueue.peek();

        const firstBlockNum = newestBlock.evmBlockNumber;
        logger.debug(`Peek result evm${newestBlock.evmBlockNumber}`);
        logger.debug(`Looking for evm${this.lastOrderedBlock + 1}...`);

        this.maybeHandleFork(newestBlock);

        while(newestBlock.evmBlockNumber == this.lastOrderedBlock + 1) {

            const evmTxs = newestBlock.evmTxs;

            const transactionsRoot = generateTxRootHash(evmTxs);
            const receiptsRoot = generateReceiptRootHash(evmTxs);
            const bloom = generateBloom(evmTxs);

            const blockTimestamp = moment.utc(newestBlock.blockTimestamp);

            // generate 'valid' block header
            const blockHeader = BlockHeader.fromHeaderData({
                'parentHash': Buffer.from(this.prevHash, 'hex'),
                'transactionsTrie': transactionsRoot,
                'receiptTrie': receiptsRoot,
                'bloom': bloom,
                'number': new BN(newestBlock.evmBlockNumber),
                'gasLimit': new BN(1000000000),
                'gasUsed': getBlockGasUsed(evmTxs),
                'difficulty': new BN(0),
                'timestamp': new BN(blockTimestamp.unix()),
                'extraData': Buffer.from(newestBlock.nativeBlockHash, 'hex')
            })

            const currentBlockHash = blockHeader.hash().toString('hex');

            // generate storeable block info
            const storableActions: StorageEosioAction[] = [];
            const storableBlockInfo: IndexedBlockInfo = {
                "transactions": storableActions,
                "errors": newestBlock.errors,
                "delta": {
                    "@timestamp": blockTimestamp.format(),
                    "block_num": newestBlock.nativeBlockNumber,
                    "code": "eosio",
                    "table": "global",
                    "@global": {
                        "block_num": newestBlock.evmBlockNumber
                    },
                    "@evmBlockHash": currentBlockHash,
                    "@receiptsRootHash": receiptsRoot.toString('hex')
                },
                "nativeHash": newestBlock.nativeBlockHash.toLowerCase(),
                "parentHash": this.prevHash,
                "transactionsRoot": transactionsRoot.toString('hex'),
                "receiptsRoot": receiptsRoot.toString('hex'),
                "blockBloom": bloom.toString('hex')
            };

            if (evmTxs.length > 0) {
                for (const [i, evmTxData] of evmTxs.entries()) {
                    evmTxData.evmTx.block_hash = currentBlockHash;
                    delete evmTxData.evmTx['raw'];
                    storableActions.push({
                        "@timestamp": newestBlock.blockTimestamp,
                        "trx_id": evmTxData.trx_id,
                        "action_ordinal": evmTxData.action_ordinal,
                        "signatures": evmTxData.signatures,
                        "@raw": evmTxData.evmTx
                    });
                }
            }

            // push to db
            this.connector.pushBlock(storableBlockInfo);

            this.prevHash = currentBlockHash;
            this.blocksQueue.dequeue();
            this.lastOrderedBlock = newestBlock.evmBlockNumber;
            this.lastNativeOrderedBlock = newestBlock.nativeBlockNumber;
            this.pushedLastSecond++;

            if (this.blocksQueue.length > 0) {
                newestBlock = this.blocksQueue.peek();
                this.maybeHandleFork(newestBlock);
            }
        }

        const blocksPushed = this.lastOrderedBlock - firstBlockNum;
        if (blocksPushed > 0)
            logger.debug(`pushed  ${blocksPushed} blocks, range: ${firstBlockNum}-${this.lastOrderedBlock}`)


        this.ordering = false;
    }

    async consumer(block: ProcessedBlock): Promise<void> {

        this.blocksQueue.queue(block);
        this.queuedUpLastSecond++;

        if (this.state == IndexerState.HEAD)
            return;

        // worker catch up machinery
        while(block.evmBlockNumber - this.lastOrderedBlock >= this.config.perf.maxBlocksBehind) {
            this.idleWorkers++;
            await sleep(200);
            this.idleWorkers--;
        }
    }

    async launch() {
        let startBlock = this.startBlock;
        let startEvmBlock = this.startBlock - this.config.evmDelta;
        let stopBlock = this.stopBlock;
        let prevHash;

        await this.connector.init();

        logger.info('checking db for blocks...');
        let lastBlock = await this.connector.getLastIndexedBlock();

        if (lastBlock != null) {
            const gaps = await this.connector.fullGapCheck();
            if (gaps.length == 0) {
                ({ startBlock, startEvmBlock, prevHash } = await this.getBlockInfoFromLastBlock(lastBlock));
            } else {
                ({ startBlock, startEvmBlock, prevHash } = await this.getBlockInfoFromGaps(gaps));
            }
        } else {
            prevHash = await this.getPreviousHash();
            logger.info(`start from ${startBlock} with hash 0x${prevHash}.`);
        }

        this.prevHash = prevHash;
        this.lastOrderedBlock = startEvmBlock - 1;
        this.lastNativeOrderedBlock = this.startBlock - 1;

        this.reader.consume(this.consumer.bind(this));

        this.reader.startProcessing({
            start_block_num: startBlock,
            end_block_num: stopBlock,
            max_messages_in_flight: this.config.perf.maxMsgsInFlight,
            irreversible_only: false,
            have_positions: [],
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true
        }, ['contract_row', 'contract_table']);

        setInterval(this.orderer.bind(this), 400);

        // debug
        setInterval(this.updateDebugStats.bind(this), 1000);

    }

    private async getGenesisBlock() {
        let genesisBlock = null;
        while(genesisBlock == null) {
            try {
                // get genesis information
                genesisBlock = await this.reader.rpc.get_block(
                    this.evmDeployBlock - 1);

            } catch (e) {
                logger.error(e);
                logger.warn(`couldn\'t get genesis block ${this.evmDeployBlock - 1} retrying in 5 sec...`);
                await sleep(5000);
                continue
            }
        }
        return genesisBlock;
    }

    private async getBlockInfoFromLastBlock(lastBlock: StorageEosioDelta): Promise<StartBlockInfo> {

        // found blocks on the database
        logger.info(JSON.stringify(lastBlock, null, 4));

        let startBlock = lastBlock.block_num - 3;
        let startEvmBlock = lastBlock['@global'].block_num - 3;

        logger.info(`purge blocks newer than ${startBlock}`);

        await this.connector.purgeNewerThan(startBlock, startEvmBlock);

        logger.info('done.');

        lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        logger.info(
            `found! ${startBlock} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return { startBlock, startEvmBlock, prevHash };
    }

    private async getBlockInfoFromGaps(gaps: Array<any>): Promise<StartBlockInfo> {

        const firstBlock = await this.connector.getIndexedBlock(gaps[0][0]);

        // found blocks on the database
        logger.info(JSON.stringify(firstBlock, null, 4));

        let startBlock = firstBlock.block_num - 1;
        let startEvmBlock = firstBlock['@global'].block_num - 1;

        logger.info(`purge blocks newer than ${startBlock}`);

        await this.connector.purgeNewerThan(startBlock, startEvmBlock);

        logger.info('done.');

        const lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        logger.info(
            `found! ${startBlock} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return { startBlock, startEvmBlock, prevHash };
    }

    private async getPreviousHash(): Promise<string> {
        // prev blocks not found, start from genesis or EVM_PREV_HASH
        if (this.config.startBlock == this.config.evmDeployBlock) {
            let genesisBlock = await this.getGenesisBlock();

            logger.info('evm deployment native genesis block: ');
            logger.info(JSON.stringify(genesisBlock, null, 4));

            // number of seconds since epoch
            const genesisTimestamp = moment.utc(genesisBlock.timestamp).unix();

            const header = BlockHeader.fromHeaderData({
                'gasLimit': new BN(0),
                'number': new BN(this.evmDeployBlock - this.config.evmDelta - 1),
                'difficulty': new BN(0),
                'timestamp': new BN(genesisTimestamp),
                'extraData': Buffer.from(genesisBlock.id, 'hex'),
                'stateRoot': Buffer.from('56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421', 'hex')
            })

            this.ethGenesisHash = header.hash().toString('hex');

            logger.info('ethereum genesis block header: ');
            logger.info(JSON.stringify(header.toJSON(), null, 4));

            logger.info(`ethereum genesis hash: 0x${this.ethGenesisHash}`);
            return this.ethGenesisHash;
        } else if (this.config.evmPrevHash != '') {
            return this.config.evmPrevHash;
        } else {
            throw new Error('Configuration error, no way to get previous hash.  Must either start from genesis or provide a previous hash via config');
        }
    }

    private maybeHandleFork(b: ProcessedBlock) {
        if (b.nativeBlockNumber > this.lastNativeOrderedBlock)
            return;

        logger.info('chain fork detected. reverse all blocks which were affected');

        // wait until all db connector write tasks finish
        while (this.connector.writeCounter > 0) {
            logger.debug(`waiting for ${this.connector.writeCounter} write operations to finish...`);
            sleep(200).then();
        }

        // clear blocksQueue
        let iterB = this.blocksQueue.peek();
        while (this.blocksQueue.length > 0 &&
               iterB.nativeBlockNumber <= this.lastNativeOrderedBlock) {
            this.blocksQueue.dequeue();
            logger.debug(`deleted ${iterB.nativeBlockNumber} from blocksQueue`);
            iterB = this.blocksQueue.peek();
        }

        // finally purge db
        this.connector.purgeNewerThan(b.nativeBlockNumber, b.evmBlockNumber).then();
        logger.debug(`purged db of blocks newer than ${b.nativeBlockNumber}, continue...`);
    }
};

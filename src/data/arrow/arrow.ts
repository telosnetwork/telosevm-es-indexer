import {
    IndexedAccountDelta, IndexedAccountDeltaSchema,
    IndexedAccountStateDelta, IndexedAccountStateDeltaSchema,
    IndexedBlock,
    IndexedBlockHeader,
    IndexedTx,
    IndexedTxSchema
} from "../../types/indexer.js";
import {BlockScroller, Connector} from "../connector.js";
import {BLOCK_GAS_LIMIT} from "../../utils/evm.js";

import cloneDeep from "lodash.clonedeep";
import {ArrowConnectorConfig, ConnectorConfig} from "../../types/config.js";
import {
    ArrowBatchContextDef, ArrowBatchReader,
    ArrowBatchWriter,
} from "@guilledk/arrowbatch-nodejs";


export const translatorDataContext: ArrowBatchContextDef = {
        alias: 'blocks',
        ordinal: 'evm_block_num',
        stream_size: '1024mb',
        map: [
            {name: 'block_num',           type: 'u64'},
            {name: 'evm_block_num',       type: 'u64'},
            {name: 'timestamp',           type: 'u64'},
            {name: 'block_hash',          type: 'checksum256'},
            {name: 'evm_block_hash',      type: 'checksum256'},
            {name: 'evm_prev_block_hash', type: 'checksum256'},
            {name: 'receipts_hash',       type: 'checksum256'},
            {name: 'txs_hash',            type: 'checksum256'},
            {name: 'gas_used',            type: 'uintvar'},
            {name: 'logs_bloom',          type: 'bytes'},
            {name: 'txs_amount',          type: 'u32'},
            {name: 'size',                type: 'u32'},
            {name: 'transactions',        type: 'struct', array: true},
            {name: 'account_deltas',      type: 'struct', array: true},
            {name: 'accountstate_deltas', type: 'struct', array: true},
            {name: 'gas_price_events',    type: 'struct', array: true},
            {name: 'revision_events',     type: 'struct', array: true},
            {name: 'openwallet_events',   type: 'struct', array: true},
            {name: 'create_events',       type: 'struct', array: true}
        ]
};


export class ArrowBlockScroller extends BlockScroller {

    private lastBlock: bigint;

    constructor(
        connector: ArrowConnector,
        params: {
            from: bigint,
            to: bigint,
            tag: string
            logLevel?: string,
            validate?: boolean
        }
    ) {
        super(connector, params);
        this.lastBlock = params.from - 1n;
    }

    async init(): Promise<void> {}

    async nextResult(): Promise<IndexedBlock> {
       const nextBlock = await this.connector.getIndexedBlock(this.lastBlock + 1n);
       this.lastBlock++;
       return nextBlock;
    }
}

export class ArrowConnector extends Connector {

    readonly pconfig: ArrowConnectorConfig;

    private readonly context: ArrowBatchWriter | ArrowBatchReader;

    constructor(config: ConnectorConfig, readOnly: boolean = false) {
        super(config);

        if (!config.arrow)
            throw new Error(`Tried to init polars connector with null config`);

        this.pconfig = config.arrow;

        const dataContext = cloneDeep(translatorDataContext);
        if (!readOnly)
            this.context = new ArrowBatchWriter(config.arrow, dataContext, this.logger);

        else
            this.context = new ArrowBatchReader(config.arrow, dataContext, this.logger);
    }

    blockFromRow(row: any[]): IndexedBlock {

        const transactions = row[12].map(tx => IndexedTxSchema.parse(tx));
        const account = row[13].map(d => IndexedAccountDeltaSchema.parse(d));
        const accountstate = row[14].map(d => IndexedAccountStateDeltaSchema.parse(d));

        const gasPriceEvents = row[15];
        const revisionEvents = row[16];
        const openWalletEvents = row[17];
        const createEvents = row[18];

        const bloom = row[9];

        return {
            timestamp: row[2],

            blockNum: row[0],
            blockHash: row[3],

            evmBlockNum: row[1],
            evmBlockHash: row[4],
            evmPrevHash: row[5],

            receiptsRoot: row[6],
            transactionsRoot: row[7],
            gasUsed: row[8],
            gasLimit: BLOCK_GAS_LIMIT,
            transactionAmount: row[10],
            size: BigInt(row[11]),

            transactions,
            logsBloom: bloom,
            gasPriceEvents,
            revisionEvents,
            openWalletEvents,
            createEvents,
            deltas: {
                account,
                accountstate
            }
        }
    }

    private rowFromBlock(block: IndexedBlock) {
        const txs = block.transactions.map(tx => {
            return {
                hash: tx.hash,
                raw: tx.raw
            };
        });
        const accountDeltas = block.deltas.account;
        const accountStateDeltas = block.deltas.accountstate;

        const blockRow = [
            block.blockNum,
            block.evmBlockNum,
            block.timestamp,
            block.blockHash,
            block.evmBlockHash,
            block.evmPrevHash,
            block.receiptsRoot,
            block.transactionsRoot,
            block.gasUsed,
            block.logsBloom,
            block.transactionAmount,
            Number(block.size),
            txs,
            accountDeltas,
            accountStateDeltas,
            block.gasPriceEvents,
            block.revisionEvents,
            block.openWalletEvents,
            block.createEvents
        ];

        return blockRow;
    }

    async init(): Promise<bigint | null> {
        await this.context.init(
            this.config.chain.startBlock);
        return null;
    }

    async deinit(): Promise<void> {
        await this.flush();

        if (this.context instanceof ArrowBatchWriter)
            await this.context.deinit();
    }

    async getAccountDeltasForBlock(blockNum: bigint): Promise<IndexedAccountDelta[]> {
        const block = await this.getIndexedBlock(blockNum);
        return block.deltas.account;
    }

    async getAccountStateDeltasForBlock(blockNum: bigint): Promise<IndexedAccountStateDelta[]> {
        const block = await this.getIndexedBlock(blockNum);
        return block.deltas.accountstate;
    }

    async getBlockHeader(blockNum: bigint): Promise<IndexedBlockHeader | null> {
        const block = await this.getIndexedBlock(blockNum);
        delete block.deltas;
        delete block.transactions;
        return block;
    }

    async getTransactionsForBlock(blockNum: bigint): Promise<IndexedTx[]> {
        const block = await this.getIndexedBlock(blockNum);
        return block.transactions;
    }

    async getFirstIndexedBlock(): Promise<IndexedBlock | null> {
        if (!this.context.firstOrdinal)
            return null;

        const rootRow = await this.context.getRow(this.context.firstOrdinal);
        return this.blockFromRow(rootRow);
    }

    async getIndexedBlock(blockNum: bigint): Promise<IndexedBlock | null> {
        const rootRow = await this.context.getRow(blockNum);
        return this.blockFromRow(rootRow);
    }

    async getLastIndexedBlock(): Promise<IndexedBlock> {
        if (!this.context.lastOrdinal)
            return null;

        const rootRow = await this.context.getRow(this.context.lastOrdinal);
        return this.blockFromRow(rootRow);
    }

    async purgeNewerThan(blockNum: bigint): Promise<void> {
        // if (this.context instanceof ArrowBatchWriter)
        //    await this.context.trimFrom(blockNum);
    }

    async fullIntegrityCheck(): Promise<bigint | null> { return null; }

    async flush(): Promise<void> {
        await new Promise<void>(resolve => {
            this.context.events.on('flush', () => {
                resolve();
            });
            if (this.context instanceof ArrowBatchWriter)
                this.context.beginFlush();
        });
    }

    async pushBlock(block: IndexedBlock): Promise<void> {
        if (this.lastPushedHash && block.evmPrevHash !== this.lastPushedHash) {
            throw new Error(`Attempted to build invalid chain, lastpushed hash: ${this.lastPushedHash}, current block prev hash: ${block.evmPrevHash}`);
        }
        this.context.pushRow(this.rowFromBlock(block));
        this.lastPushed = block.blockNum;
        this.lastPushedHash = block.evmBlockHash;
    }

    forkCleanup(timestamp: bigint, lastNonForked: bigint, lastForked: bigint): void {}

    blockScroll(params: {
        from: bigint;
        to: bigint;
        tag: string;
        logLevel?: string;
        validate?: boolean;
        scrollOpts?: any
    }): BlockScroller {
        return new ArrowBlockScroller(this, params);
    }
}
import {readFileSync} from "node:fs";

import {IndexedBlockInfo, TranslatorConfig, IndexerState, StartBlockInfo} from './types/indexer.js';

import {createLogger, format, Logger, transports} from 'winston';

import {StorageEosioAction, StorageEosioDelta} from './types/evm.js';

import {Connector} from './database/connector.js';

import {
    arrayToHex,
    generateBlockApplyInfo,
    hexStringToUint8Array,
    ZERO_HASH,
    ProcessedBlock, removeHexPrefix, BLOCK_GAS_LIMIT, EMPTY_TRIE, EMPTY_TRIE_BUF
} from './utils/evm.js'

import moment from 'moment';
import {getRPCClient} from './utils/eosio.js';
import {ABI, APIClient} from "@wharfkit/antelope";

import EventEmitter from "events";

import {packageInfo, sleep} from "./utils/indexer.js";

import * as evm from "@ethereumjs/common";
import {TEVMBlockHeader} from "telos-evm-custom-ds";
import {createDeposit, createEvm, createWithdraw, HandlerArguments} from "./transactions.js";

import {clearInterval} from "timers";
import {
    Block,
    StateHistoryReader,
    StateHistoryReaderOptionsSchema,
    ThroughputMeasurer
} from "@guilledk/state-history-reader";


export class TEVMTranslator {
    startBlock: number;
    stopBlock: number;

    state: IndexerState = IndexerState.SYNC;  // global indexer state, either HEAD or SYNC, changes buffered-writes-to-db machinery to be write-asap

    readonly config: TranslatorConfig;  // global indexer config as defined by envoinrment or config file

    private reader: StateHistoryReader;  // websocket state history connector, deserializes nodeos protocol
    private readonly readerAbis: {account: string, abi: ABI}[];

    private rpc: APIClient;
    private remoteRpc: APIClient;
    connector: Connector;  // custom elastic search db driver

    private prevHash: string;  // previous indexed block evm hash, needed by machinery (do not modify manualy)
    headBlock: number;
    lastBlock: number;  // last block number that was succesfully pushed to db in order

    // debug status used to print statistics
    private pushedLastUpdate: number = 0;
    private stallCounter: number = 0;
    private perfMetrics: ThroughputMeasurer = new ThroughputMeasurer({
        windowSizeMs: 10 * 1000
    });

    private perfTaskId: NodeJS.Timer;
    private stateSwitchTaskId: NodeJS.Timer;

    private logger: Logger;

    events = new EventEmitter();

    private readonly common: evm.Common;
    private _isRestarting: boolean = false;

    private sequenceHistory: StorageEosioDelta[] = [];

    constructor(config: TranslatorConfig) {
        this.startBlock = config.startBlock;
        this.stopBlock = config.stopBlock;
        this.config = config;
        this.common = evm.Common.custom({
            chainId: this.config.chainId,
            defaultHardfork: evm.Hardfork.Istanbul
        }, {baseChain: evm.Chain.Mainnet});

        this.rpc = getRPCClient(config.endpoint);
        this.remoteRpc = getRPCClient(config.remoteEndpoint);

        this.readerAbis = ['eosio', 'eosio.token', 'eosio.msig', 'eosio.evm'].map(abiName => {
            const jsonAbi = JSON.parse(readFileSync(`src/abis/${abiName}.json`).toString());
            return {account: jsonAbi.account_name, abi: ABI.from(jsonAbi.abi)};
        });

        process.on('SIGINT', async () => await this.stop());
        process.on('SIGUSR1', async () => await this.resetReader());
        process.on('SIGQUIT', async () => await this.stop());
        process.on('SIGTERM', async () => await this.stop());

        process.on('unhandledRejection', error => {
            // @ts-ignore
            if (error.message == 'Worker terminated')
                return;
            this.logger.error('Unhandled Rejection');
            try {
                this.logger.error(JSON.stringify(error, null, 4));
            } catch (e) {

            }
            // @ts-ignore
            this.logger.error(error.message);
            // @ts-ignore
            this.logger.error(error.stack);
            throw error;
        });

        const loggingOptions = {
            exitOnError: false,
            level: this.config.logLevel,
            format: format.combine(
                format.metadata(),
                format.colorize(),
                format.timestamp(),
                format.printf((info: any) => {
                    return `${info.timestamp} [PID:${process.pid}] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                })
            )
        }
        this.logger = createLogger(loggingOptions);
        this.logger.add(new transports.Console({
            level: this.config.logLevel
        }));
        this.logger.debug('Logger initialized with level ' + this.config.logLevel);
        this.connector = new Connector(this.config, this.logger);
    }

    /*
     * Debug routine that prints indexing stats, periodically called every second
     */
    async performanceMetricsTask() {

        if (this.perfMetrics.max > 0) {
            if (this.perfMetrics.average == 0)
                this.stallCounter++;

            if (this.stallCounter > this.config.perf.stallCounter) {
                this.logger.info('stall detected... restarting ship reader.');
                await this.resetReader();
            }
        }

        const avgSpeed = this.perfMetrics.average.toFixed(2);
        let statsString = `${this.lastBlock.toLocaleString()} pushed, at ${avgSpeed} blocks/sec avg`;

        this.logger.info(statsString);

        this.perfMetrics.measure(this.pushedLastUpdate);
        this.pushedLastUpdate = 0;
    }

    async resetReader() {
        this.logger.warn("restarting SHIP reader!...");
        this._isRestarting = true;
        this.stallCounter = -2;
        await this.reader.restart(1000, this.lastBlock + 1);
        this._isRestarting = false;
    }

    /*
     * Generate valid ethereum has, requires blocks to be passed in order, updates state
     * handling class attributes.
     */
    async hashBlock(block: ProcessedBlock): Promise<IndexedBlockInfo> {
        const evmTxs = block.evmTxs;

        // generate block info derived from applying the transactions to the vm state
        const blockApplyInfo = await generateBlockApplyInfo(evmTxs);
        const blockTimestamp = moment.utc(block.blockTimestamp);

        // generate 'valid' block header
        const blockHeader = TEVMBlockHeader.fromHeaderData({
            'parentHash': hexStringToUint8Array(this.prevHash),
            'stateRoot': EMPTY_TRIE_BUF,
            'transactionsTrie': blockApplyInfo.txsRootHash.root(),
            'receiptTrie': blockApplyInfo.receiptsTrie.root(),
            'logsBloom': blockApplyInfo.blockBloom.bitvector,
            'number': BigInt(block.evmBlockNumber),
            'gasLimit': BLOCK_GAS_LIMIT,
            'gasUsed': blockApplyInfo.gasUsed,
            'timestamp': BigInt(blockTimestamp.unix()),
            'extraData': hexStringToUint8Array(block.nativeBlockHash)
        }, {common: this.common});

        const currentBlockHash = arrayToHex(blockHeader.hash());
        const receiptsHash = arrayToHex(blockApplyInfo.receiptsTrie.root());
        const txsHash = arrayToHex(blockApplyInfo.txsRootHash.root());

        // generate storeable block info
        const storableActions: StorageEosioAction[] = [];
        const storableBlockInfo: IndexedBlockInfo = {
            "transactions": storableActions,
            "delta": {
                "@timestamp": blockTimestamp.format(),
                "block_num": block.nativeBlockNumber,
                "@global": {
                    "block_num": block.evmBlockNumber
                },
                "@evmPrevBlockHash": this.prevHash,
                "@evmBlockHash": currentBlockHash,
                "@blockHash": block.nativeBlockHash,
                "@receiptsRootHash": receiptsHash,
                "@transactionsRoot": txsHash,
                "gasUsed": blockApplyInfo.gasUsed.toString(),
                "gasLimit": BLOCK_GAS_LIMIT.toString(),
                "size": blockApplyInfo.size.toString()
            },
            "nativeHash": block.nativeBlockHash.toLowerCase(),
            "parentHash": this.prevHash,
            "receiptsRoot": receiptsHash,
            "blockBloom": arrayToHex(blockApplyInfo.blockBloom.bitvector)
        };

        if (evmTxs.length > 0) {
            for (const evmTxData of evmTxs) {
                evmTxData.evmTx.block_hash = currentBlockHash;
                delete evmTxData.evmTx['raw'];
                storableActions.push({
                    "@timestamp": block.blockTimestamp,
                    "trx_id": evmTxData.trx_id,
                    "action_ordinal": evmTxData.action_ordinal,
                    "@raw": evmTxData.evmTx
                });

            }
        }

        this.prevHash = currentBlockHash;

        return storableBlockInfo;
    }

    private async handleStateSwitch() {
        // SYNC & HEAD mode switch detection
        try {
            this.headBlock = ((await this.remoteRpc.v1.chain.get_info()).head_block_num).toNumber();
            const isHeadTarget = this.headBlock >= this.stopBlock;
            const targetBlock = isHeadTarget ? this.headBlock : this.stopBlock;

            const blocksUntilHead = targetBlock - this.lastBlock;

            let statsString = `${blocksUntilHead.toLocaleString()} until target block ${targetBlock.toLocaleString()}`;
            if (this.perfMetrics.max != 0)
                statsString += ` ETA: ${moment.duration(blocksUntilHead / this.perfMetrics.average, 'seconds').humanize()}`;

            this.logger.info(statsString);

            if (isHeadTarget && blocksUntilHead <= 100) {
                if (this.state == IndexerState.HEAD)
                    return;

                this.state = IndexerState.HEAD;
                this.connector.state = IndexerState.HEAD;

                this.logger.info(
                    'switched to HEAD mode! blocks will be written to db asap.');

            } else {
                if (this.state == IndexerState.SYNC)
                    return;

                this.state = IndexerState.SYNC;
                this.connector.state = IndexerState.SYNC;

                this.logger.info(
                    'switched to SYNC mode! blocks will be written to db in batches.');
            }

        } catch (error) {
            this.logger.warn('get_info query to remote failed with error:');
            this.logger.warn(error);
        }
    }

    /*
     * StateHistoryReader emit block callback, gets blocks from ship in order.
     */
    private async processBlock(block: Block): Promise<void> {
        const currentBlock = block.status.this_block.block_num;

        if (this._isRestarting) {
            this.logger.warn(`dropped ${currentBlock} due to restart...`);
            return;
        }

        if (currentBlock < this.startBlock) {
            this.reader.ack();
            this.lastBlock = currentBlock;
            return;
        }

        if (this.stopBlock > 0 && currentBlock > this.stopBlock)
            return;

        if (currentBlock > this.lastBlock + 1) {
            this.logger.warn(`Expected block ${this.lastBlock + 1} and got ${currentBlock}, gap on reader?`);
            await this.resetReader();
            return;
        }

        this.stallCounter = 0;

        // native-evm block num delta is constant based on config
        const currentEvmBlock = currentBlock - this.config.evmBlockDelta;
        const errors = []

        // traces
        const systemAccounts = ['eosio', 'eosio.stake', 'eosio.ram'];
        const contractWhitelist = [
            "eosio.evm", "eosio.token",  // evm
            "eosio.msig"  // deferred transaction sig catch
        ];
        const actionWhitelist = [
            "raw", "withdraw", "transfer",  // evm
            "exec" // msig deferred sig catch
        ]
        const actions = [];
        const evmTxs = [];
        const evmTransactions = [];

        for (const action of block.actions) {
            // const aDuplicate = actions.find(other => {
            //     return other.receipt.act_digest === action.receipt.act_digest
            // })
            // if (aDuplicate)
            //     continue;

            if (!contractWhitelist.includes(action.act.account) ||
                !actionWhitelist.includes(action.act.name))
                continue;

            const isEvmContract = action.act.account === 'eosio.evm';
            const isRaw = isEvmContract && action.act.name === 'raw';
            const isWithdraw = isEvmContract && action.act.name === 'withdraw';

            const isTokenContract = action.act.account === 'eosio.token';
            const isTransfer = isTokenContract && action.act.name === 'transfer';
            const isDeposit = isTransfer && action.act.data.to === 'eosio.evm';

            // discard transfers to accounts other than eosio.evm
            // and transfers from system accounts
            if ((isTransfer && action.receiver != 'eosio.evm') ||
                (isTransfer && action.act.data.from in systemAccounts))
                continue;

            const params: HandlerArguments = {
                nativeBlockHash: block.status.this_block.block_id,
                trx_index: evmTxs.length,
                blockNum: currentEvmBlock,
                tx: action.act.data,
                consoleLog: action.console
            };

            if (isRaw)
                evmTxs.push(createEvm(params, this.common, this.logger));

            else if (isWithdraw)
                evmTxs.push(await createWithdraw(params, this.common, this.rpc, this.logger));

            else if (isDeposit)
                evmTxs.push(await createDeposit(params, this.common, this.rpc, this.logger));

            else
                continue;

            actions.push(action);
        }

        let gasUsedBlock = BigInt(0);
        let i = 0;
        for (const evmTx of evmTxs) {
            gasUsedBlock += BigInt(evmTx.gasused);
            evmTx.gasusedblock = gasUsedBlock.toString();

            const action = actions[i];
            evmTransactions.push({
                action_ordinal: action.actionOrdinal,
                trx_id: action.trxId,
                evmTx: evmTx
            });
            i++;
        }

        const newestBlock = new ProcessedBlock({
            nativeBlockHash: block.status.this_block.block_id,
            nativeBlockNumber: currentBlock,
            evmBlockNumber: currentEvmBlock,
            blockTimestamp: block.header.timestamp,
            evmTxs: evmTransactions,
            errors: errors
        });

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // fork handling
        if (currentBlock != this.lastBlock + 1)
            await this.handleFork(newestBlock);

        const storableBlockInfo = await this.hashBlock(newestBlock);

        // remember block sequence & trim if older than lib
        this.sequenceHistory.push(storableBlockInfo.delta);
        while (this.sequenceHistory.length > 0 &&
               this.sequenceHistory[0].block_num < block.status.last_irreversible.block_num)
            this.sequenceHistory.shift();

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // Update block num state tracking attributes
        this.lastBlock = currentBlock;

        // Push to db
        await this.connector.pushBlock(storableBlockInfo);
        this.events.emit('push-block', storableBlockInfo);

        if (currentBlock == this.stopBlock) {
            await this.stop();
            this.events.emit('stop');
            return;
        }

        // For debug stats
        this.pushedLastUpdate++;

        this.reader.ack();
    }

    getOldHash(blockNum: number) {
        let block: StorageEosioDelta = null;
        let sequenceIndex = this.sequenceHistory.length - 1;
        while (sequenceIndex >= 0) {
            if(this.sequenceHistory[sequenceIndex].block_num == blockNum) {
                block = this.sequenceHistory[sequenceIndex];
                break;
            }
            sequenceIndex--;
        }

        if(!block)
            throw new Error(`Block #${blockNum} not found in short term memory`);
        return {
            prevHash: block['@evmBlockHash'],
            index: sequenceIndex
        };
    }

    async startReaderFrom(blockNum: number) {
        this.reader = new StateHistoryReader(StateHistoryReaderOptionsSchema.parse({
            shipAPI: this.config.wsEndpoint,
            chainAPI: this.config.endpoint,
            startBlock: blockNum,
            stopBlock: this.config.stopBlock,
            actionWhitelist: {
                'eosio.token': ['transfer'],
                'eosio.msig': ['exec'],
                'eosio.evm': ['raw', 'withdraw']
            },
            tableWhitelist: {},
            irreversibleOnly: this.config.irreversibleOnly,
            logLevel: (this.config.readerLogLevel || 'info').toLowerCase(),
            maxMsgsInFlight: this.config.perf.maxMessagesInFlight || 10000,
            maxPayloadMb: Math.floor(1024 * 1.5)
        }));

        this.reader.addContracts(this.readerAbis);

        this.reader.onConnected = () => {
            this.logger.info('SHIP Reader connected.');
        }
        this.reader.onDisconnect = () => {
            this.logger.warn('SHIP Reader disconnected.');
        }
        this.reader.onError = (err) => {
            this.logger.error(`SHIP Reader error: ${err}`);
            this.logger.error(err.stack);
        }
        this.reader.onBlock = this.processBlock.bind(this);

        await this.reader.start();
    }

    /*
     * Entry point
     */
    async launch() {
        this.printIntroText();

        let startBlock = this.startBlock;
        let prevHash: string;

        await this.connector.init();

        if (this.config.runtime.trimFrom) {
            const trimBlockNum = this.config.runtime.trimFrom;
            await this.connector.purgeNewerThan(trimBlockNum);
        }

        this.logger.info('checking db for blocks...');
        let lastBlock = await this.connector.getLastIndexedBlock();

        let gap = null;
        if (!this.config.runtime.skipIntegrityCheck) {
            if (lastBlock != null) {
                this.logger.debug('performing integrity check...');
                gap = await this.connector.fullIntegrityCheck();

                if (gap == null) {
                    this.logger.info('NO GAPS FOUND');
                } else {
                    this.logger.info('GAP INFO:');
                    this.logger.info(JSON.stringify(gap, null, 4));
                }
            } else {
                if (this.config.runtime.onlyDBCheck) {
                    this.logger.warn('--only-db-check on empty database...');
                }
            }
        }

        if (this.config.runtime.onlyDBCheck) {
            this.logger.info('--only-db-check passed exiting...');
            await this.connector.deinit();
            return;
        }

        if (lastBlock != null &&
            lastBlock['@evmPrevBlockHash'] != ZERO_HASH) {
            // if there is a last block found on db other than genesis doc

            if (gap == null) {
                ({startBlock, prevHash} = await this.getBlockInfoFromLastBlock(lastBlock));
            } else {
                if (this.config.runtime.gapsPurge)
                    ({startBlock, prevHash} = await this.getBlockInfoFromGap(gap));
                else
                    throw new Error(
                        `Gap found in database at ${gap}, but --gaps-purge flag not passed!`);
            }

            this.prevHash = prevHash;
            this.startBlock = startBlock;
            this.lastBlock = startBlock - 1;
            this.connector.lastPushed = this.lastBlock;

        } else if (this.config.evmPrevHash != '') {
            // if there is an evmPrevHash set state directly
            prevHash = this.config.evmPrevHash;
            this.prevHash = this.config.evmPrevHash;
            this.startBlock = this.config.startBlock;
            this.lastBlock = startBlock - 1;
            this.connector.lastPushed = this.lastBlock;
        }

        if (prevHash)
            this.logger.info(`start from ${startBlock} with hash 0x${prevHash}.`);
        else {
            this.logger.info(`starting from genesis block ${startBlock}`);
            await this.genesisBlockInitialization();
        }

        if (!this.config.runtime.skipStartBlockCheck) {
            // check node actually contains first block
            await this.rpc.v1.chain.get_block(startBlock);
        }

        if (!this.config.runtime.skipRemoteCheck) {
            // check remote node is up
            try {
                await this.remoteRpc.v1.chain.get_info();
            } catch (error) {
                this.logger.error(`Error while doing remote node check: ${error.message}`);
                throw error;
            }
        }

        this.logger.info('Initializing ws broadcast...')
        this.connector.startBroadcast();

        await this.startReaderFrom(startBlock);

        // Launch bg routines
        this.perfTaskId = setInterval(async () => await this.performanceMetricsTask(), 1000);
        this.stateSwitchTaskId = setInterval(() => this.handleStateSwitch(), 10 * 1000);
    }

    async genesisBlockInitialization() {
        const genesisBlock = await this.getGenesisBlock();

        // number of seconds since epoch
        const genesisTimestamp = moment.utc(genesisBlock.timestamp).unix();

        // genesis evm block num
        const genesisEvmBlockNum = genesisBlock.block_num - this.config.evmBlockDelta;

        const genesisHeader = TEVMBlockHeader.fromHeaderData({
            'number': BigInt(genesisEvmBlockNum),
            'stateRoot': EMPTY_TRIE_BUF,
            'gasLimit': BLOCK_GAS_LIMIT,
            'timestamp': BigInt(genesisTimestamp),
            'extraData': hexStringToUint8Array(genesisBlock.id)
        }, {common: this.common});

        const genesisHash = arrayToHex(genesisHeader.hash());

        if (this.config.evmValidateHash != "" &&
            genesisHash != this.config.evmValidateHash) {
            throw new Error('FATAL!: Generated genesis hash doesn\'t match remote!');
        }

        // Init state tracking attributes
        this.prevHash = genesisHash;
        this.lastBlock = genesisBlock.block_num;
        this.connector.lastPushed = this.lastBlock;

        this.logger.info('ethereum genesis header: ');
        this.logger.info(JSON.stringify(genesisHeader.toJSON(), null, 4));

        this.logger.info(`ethereum genesis hash: 0x${genesisHash}`);

        // if we are starting from genesis store block skeleton doc
        // for rpc to be able to find parent hash for fist block
        await this.connector.pushBlock({
            transactions: [],
            delta: {
                '@timestamp': moment.utc(genesisBlock.timestamp).toISOString(),
                block_num: genesisBlock.block_num,
                '@global': {
                    block_num: genesisEvmBlockNum
                },
                '@blockHash': genesisBlock.id.toLowerCase(),
                '@evmPrevBlockHash': removeHexPrefix(ZERO_HASH),
                '@evmBlockHash': genesisHash,
                "@receiptsRootHash": EMPTY_TRIE,
                "@transactionsRoot": EMPTY_TRIE,
                "gasUsed": "0",
                "gasLimit": BLOCK_GAS_LIMIT.toString(),
                "size": "0"
            },
            nativeHash: genesisBlock.id.toLowerCase(),
            parentHash: '',
            receiptsRoot: '',
            blockBloom: ''
        });

        this.events.emit('start');
    }

    /*
     * Wait until all db connector write tasks finish
     */
    async _waitWriteTasks() {
        if (!this.connector)
            return;

        while (this.connector.writeCounter > 0) {
            this.logger.debug(`waiting for ${this.connector.writeCounter} write operations to finish...`);
            await sleep(200);
        }
    }

    /*
     * Stop indexer gracefully
     */
    async stop() {
        clearInterval(this.perfTaskId as unknown as number);
        clearInterval(this.stateSwitchTaskId as unknown as number);

        if (this.reader) {
            try {
                await this.reader.stop();
            } catch (e) {
                this.logger.warn(`error stopping reader: ${e.message}`);
            }
        }

        await this._waitWriteTasks();

        if (this.connector) {
            try {
                await this.connector.deinit();
            } catch (e) {
                this.logger.warn(`error stopping connector: ${e.message}`);
            }
        }

        // if (this.evmDeserializationPool) {
        //     try {
        //         await this.evmDeserializationPool.terminate(true);
        //     } catch (e) {
        //         this.logger.warn(`error stopping thread pool: ${e.message}`);
        //     }
        // }

        // if (process.env.LOG_LEVEL == 'debug')
        //     setTimeout(logWhyIsNodeRunning, 5000);
    }

    /*
     * Poll remote rpc for genesis block, which is block previous to evm deployment
     */
    private async getGenesisBlock() {
        let genesisBlock = null;
        while (genesisBlock == null) {
            try {
                // get genesis information
                genesisBlock = await this.rpc.v1.chain.get_block(
                    this.startBlock - 1);

            } catch (e) {
                this.logger.error(e);
                this.logger.warn(`couldn\'t get genesis block ${this.startBlock - 1} retrying in 5 sec...`);
                await sleep(5000);
            }
        }
        return genesisBlock;
    }

    /*
     * Get start parameters from last block indexed on db
     */
    private async getBlockInfoFromLastBlock(lastBlock: StorageEosioDelta): Promise<StartBlockInfo> {

        // sleep, then get last block again, if block_num changes it means
        // another indexer is running
        await sleep(3000);
        const newlastBlock = await this.connector.getLastIndexedBlock();
        if (lastBlock.block_num != newlastBlock.block_num)
            throw new Error(
                'New last block check failed probably another indexer is running, abort...');

        let startBlock = lastBlock.block_num;

        this.logger.info(`purge blocks newer than ${startBlock}`);

        await this.connector._purgeBlocksNewerThan(startBlock);

        this.logger.info('done.');

        lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        this.logger.info(
            `found! ${lastBlock.block_num} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return {startBlock, prevHash};
    }

    /*
     * Get start parameters from first gap on database
     */
    private async getBlockInfoFromGap(gap: number): Promise<StartBlockInfo> {

        let firstBlock: StorageEosioDelta;
        let delta = 0;
        while (!firstBlock || firstBlock.block_num === undefined) {
            firstBlock = await this.connector.getIndexedBlock(gap - delta);
            delta++;
        }
        // found blocks on the database
        this.logger.info(`Last block of continuous range found: ${JSON.stringify(firstBlock, null, 4)}`);

        let startBlock = firstBlock.block_num;

        this.logger.info(`purge blocks newer than ${startBlock}`);

        await this.connector.purgeNewerThan(startBlock);

        this.logger.info('done.');

        const lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        if (lastBlock.block_num != (startBlock - 1))
            throw new Error(`Last block: ${lastBlock.block_num}, is not ${startBlock - 1} - 1`);

        this.logger.info(
            `found! ${lastBlock} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return {startBlock, prevHash};
    }

    /*
     * Handle fork, leave every state tracking attribute in a healthy state
     */
    private async handleFork(b: ProcessedBlock) {
        const lastNonForked = b.nativeBlockNumber - 1;
        const forkedAt = this.lastBlock;

        this.logger.info(`got ${b.nativeBlockNumber} and expected ${this.lastBlock + 1}, chain fork detected. reverse all blocks which were affected`);

        await this._waitWriteTasks();

        // finally purge db
        await this.connector.purgeNewerThan(lastNonForked + 1);
        this.logger.debug(`purged db of blocks newer than ${lastNonForked}, continue...`);

        // tweak variables used by ordering machinery
        const {prevHash, index} = this.getOldHash(lastNonForked);
        this.sequenceHistory.splice(index + 1);
        this.prevHash = prevHash;
        this.lastBlock = lastNonForked;

        this.connector.forkCleanup(
            b.blockTimestamp,
            lastNonForked,
            forkedAt
        );
    }

    printIntroText() {
        this.logger.info(`Telos EVM Translator v${packageInfo.version}`);
        this.logger.info('Happy indexing!');
    }
}

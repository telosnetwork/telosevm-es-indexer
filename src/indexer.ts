import {readFileSync} from "node:fs";

import {HyperionSequentialReader, ThroughputMeasurer} from "@telosnetwork/hyperion-sequential-reader";
import {loggers, format, Logger, transports} from 'winston';
import {TEVMBlockHeader} from "telos-evm-custom-ds";
import {clearInterval} from "timers";

import {
    IndexedAccountDelta, IndexedAccountDeltaSchema, IndexedAccountStateDelta, IndexedAccountStateDeltaSchema,
    IndexedBlock, IndexedBlockSchema, IndexedConfigDelta, IndexedConfigDeltaSchema, IndexedTx,
    IndexerState,
    StartBlockInfo,
} from './types/indexer.js';
import {
    arrayToHex,
    generateBlockApplyInfo,
    isTxDeserializationError,
    BLOCK_GAS_LIMIT, EMPTY_TRIE_BUF, ZERO_HASH, EMPTY_TRIE
} from './utils/evm.js'

import moment from 'moment';
import {getRPCClient} from './utils/eosio.js';
import {ABI} from "@wharfkit/antelope";

import EventEmitter from "events";


import workerpool from 'workerpool';
import * as evm from "@ethereumjs/common";

import {APIClient} from "@wharfkit/antelope";
import {packageInfo, sleep} from "./utils/indexer.js";
import {HandlerArguments} from "./workers/handlers.js";
import {Connector} from "./data/connector.js";
import {ArrowConnector} from "./data/arrow/arrow.js";
import {DecodedBlock} from "@telosnetwork/hyperion-sequential-reader/lib/esm/types/antelope";
import {ChainConfig, TranslatorConfig} from "./types/config.js";
import {Bloom} from "@ethereumjs/vm";
import {addHexPrefix} from "@ethereumjs/util";

EventEmitter.defaultMaxListeners = 1000;

export class TEVMIndexer {
    state: IndexerState = IndexerState.SYNC;  // global indexer state, either HEAD or SYNC, changes buffered-writes-to-db machinery to be write-asap

    config: TranslatorConfig;  // global translator config as defined by environment or config file

    private readonly chain: ChainConfig;

    private reader: HyperionSequentialReader;  // websocket state history connector, deserializes nodeos protocol
    private readonly readerAbis: {account: string, abi: ABI}[];

    private rpc: APIClient;
    private remoteRpc: APIClient;
    sourceConnector: Connector;  // custom elastic search db driver
    targetConnector: Connector;

    // private prevHash: string;  // previous indexed block evm hash, needed by machinery (do not modify manualy)
    headBlock: bigint;
    lastBlock: bigint;  // last block number that was succesfully pushed to db in order

    // debug status used to print statistics
    private pushedLastUpdate: number = 0;
    private stallCounter: number = 0;
    private perfMetrics: ThroughputMeasurer = new ThroughputMeasurer({
        windowSizeMs: 10 * 1000
    });

    private perfTaskId: NodeJS.Timer;
    private stateSwitchTaskId: NodeJS.Timer;

    private readonly logger: Logger;

    events = new EventEmitter();

    private evmDeserializationPool;

    private readonly common: evm.Common;
    private _isRestarting: boolean = false;
    private _isReaderOnEBRMode: boolean;

    constructor(config: TranslatorConfig) {
        this.config = config;

        this.chain = this.config.connector.chain;

        this.common = evm.Common.custom({
            chainId: this.chain.chainId,
            defaultHardfork: evm.Hardfork.Istanbul
        }, {baseChain: evm.Chain.Mainnet});

        if (config.connector.nodeos) {
            this.rpc = getRPCClient(config.connector.nodeos.endpoint);
            this.remoteRpc = getRPCClient(config.connector.nodeos.remoteEndpoint);
            this.readerAbis = ['eosio', 'eosio.token', 'eosio.msig', 'telos.evm'].map(abiName => {
                const jsonAbi = JSON.parse(readFileSync(`src/abis/${abiName}.json`).toString());
                return {account: jsonAbi.account_name, abi: ABI.from(jsonAbi.abi)};
            });
        }

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
            ),
            transports: [
                new transports.Console({
                    level: this.config.logLevel
                })
            ]
        }
        this.logger = loggers.add('translator', loggingOptions);
        this.logger.debug('Logger initialized with level ' + this.config.logLevel);
    }

    /*
     * Debug routine that prints indexing stats, periodically called every second
     */
    async performanceMetricsTask() {

        if (this.config.connector.nodeos && this.perfMetrics.max > 0) {
            if (this.perfMetrics.average == 0)
                this.stallCounter++;

            if (this.stallCounter > this.config.connector.nodeos.stallCounter) {
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
        this.reader.restart(1000, Number(this.lastBlock) + 1);
        await new Promise<void>((resolve, reject) => {
            this.reader.events.once('restarted', () => {
                this._isRestarting = false;
                resolve();
            });
            this.reader.events.once('error', (error) => {
                reject(error);
            });
        });
    }

    /*
     * Generate valid ethereum has, requires blocks to be passed in order, updates state
     * handling class attributes.
     */
    async hashBlock(block: Partial<IndexedBlock>): Promise<{
        evmBlockHash: string,
        receiptsRoot: string,
        txsRoot: string
    }> {
        // generate block info derived from applying the transactions to the vm state
        const blockApplyInfo = await generateBlockApplyInfo(block.transactions);

        // generate 'valid' block header
        const blockHeader = TEVMBlockHeader.fromHeaderData({
            'parentHash': addHexPrefix(block.evmPrevHash),
            'stateRoot': EMPTY_TRIE_BUF,
            'transactionsTrie': blockApplyInfo.txsRootHash.root(),
            'receiptTrie': blockApplyInfo.receiptsTrie.root(),
            'logsBloom': blockApplyInfo.blockBloom.bitvector,
            'number': block.evmBlockNum,
            'gasLimit': BLOCK_GAS_LIMIT,
            'gasUsed': blockApplyInfo.gasUsed,
            'timestamp': block.timestamp,
            'extraData': addHexPrefix(block.blockHash)
        }, {common: this.common});

        const currentBlockHash = arrayToHex(blockHeader.hash());

        if (block.blockNum === BigInt(this.chain.startBlock)) {
            if (this.chain.evmValidateHash.length > 0 &&
                currentBlockHash !== this.chain.evmValidateHash) {
                this.logger.error(`Generated first block:\n${JSON.stringify(blockHeader, null, 4)}`);
                throw new Error(`initial hash validation failed: got ${currentBlockHash} and expected ${this.chain.evmValidateHash}`);
            }
        }

        return {
            evmBlockHash: Buffer.from(blockHeader.hash()).toString('hex'),
            receiptsRoot: Buffer.from(blockApplyInfo.receiptsTrie.root()).toString('hex'),
            txsRoot: Buffer.from(blockApplyInfo.txsRootHash.root()).toString('hex')
        }
    }

    private async handleStateSwitch() {
        if (!this.config.connector.nodeos)
            throw new Error('handleStateSwitch task called but not reading from nodeos');

        // SYNC & HEAD mode switch detection
        try {
            this.headBlock = BigInt((await this.remoteRpc.v1.chain.get_info()).head_block_num.toNumber());
            const isHeadTarget = this.headBlock >= this.chain.stopBlock;
            const targetBlock = isHeadTarget ? this.headBlock : BigInt(this.chain.stopBlock);

            const blocksUntilHead = Number(targetBlock - this.lastBlock);

            let statsString = `${blocksUntilHead.toLocaleString()} until target block ${targetBlock.toLocaleString()}`;
            if (this.perfMetrics.max != 0)
                statsString += ` ETA: ${moment.duration(blocksUntilHead / this.perfMetrics.average, 'seconds').humanize()}`;

            if (this._isReaderOnEBRMode)
                statsString += ' (reader on EBR mode)'

            this.logger.info(statsString);

            if (isHeadTarget && blocksUntilHead <= 100) {
                if (this.state == IndexerState.HEAD)
                    return;

                this.state = IndexerState.HEAD;
                this.targetConnector.state = IndexerState.HEAD;

                this.logger.info(
                    'switched to HEAD mode! blocks will be written to db asap.');

            } else {
                if (this.state == IndexerState.SYNC)
                    return;

                this.state = IndexerState.SYNC;
                this.targetConnector.state = IndexerState.SYNC;

                this.logger.info(
                    'switched to SYNC mode! blocks will be written to db in batches.');
            }

        } catch (error) {
            this.logger.warn('get_info query to remote failed with error:');
            this.logger.warn(error);
        }
    }

    /*
     * HyperionSequentialReader emit block callback, gets blocks from ship in order.
     */
    async processSHIPBlock(prevHash: string, block: DecodedBlock): Promise<IndexedBlock> {
        const currentBlock = BigInt(block.blockInfo.this_block.block_num);

        if (this._isRestarting) {
            this.logger.warn(`dropped ${currentBlock} due to restart...`);
            return;
        }

        if (currentBlock < this.chain.startBlock) {
            this.reader.ack();
            return;
        }

        if (this.chain.stopBlock > 0 && currentBlock > this.chain.stopBlock)
            return;

        if (currentBlock > this.lastBlock + 1n) {
            this.logger.warn(`Expected block ${this.lastBlock + 1n} and got ${currentBlock}, gap on reader?`);
            await this.resetReader();
            return;
        }

        this.stallCounter = 0;

        // native-evm block num delta is constant based on config
        const currentEvmBlock = currentBlock - BigInt(this.chain.evmBlockDelta);
        const blockTimestamp = BigInt(moment.utc(block.blockHeader.timestamp).unix());

        // traces
        const systemAccounts = ['eosio', 'eosio.stake', 'eosio.ram'];
        const contractWhitelist = [
            "eosio.evm", "eosio.token",  // evm
            "eosio.msig"  // deferred transaction sig catch
        ];
        const actionWhitelist = [
            "raw", "withdraw", "transfer", "doresources", "create", "openwallet", "setrevision", // evm
            "exec" // msig deferred sig catch
        ]

        const actions = [];
        const accountDeltas: IndexedAccountDelta[] = [];
        const stateDeltas: IndexedAccountStateDelta[] = [];

        const configChanges: IndexedConfigDelta[] = [];
        let evmTxIndex = 0;

        const gasPriceEvents = [];
        const revisionEvents = [];
        const openWalletEvents =[];
        const createEvents = [];

        let d = 0;
        block.deltas.forEach(delta => {
            const indexedDelta = {
                timestamp: blockTimestamp,
                blockNum: BigInt(currentBlock),
                ordinal: d,
                ...delta.value
            }

            if (indexedDelta.code)
                indexedDelta.code = arrayToHex(indexedDelta.code);

            if (delta.table == 'account')
                accountDeltas.push(IndexedAccountDeltaSchema.parse(indexedDelta));

            if (delta.table == 'accountstate') {
                indexedDelta.scope = delta.scope;
                stateDeltas.push(IndexedAccountStateDeltaSchema.parse(indexedDelta));
            }

            if (delta.table == 'config') {
                configChanges.push(IndexedConfigDeltaSchema.parse(indexedDelta));
            }

            d++;
        });

        let evmTxs = [];
        const txTasks = [];
        const startTxTask = (taskType: string, params: HandlerArguments) => {
                txTasks.push(
                    this.evmDeserializationPool.exec(taskType, [params])
                        .catch((err) => {
                            this.logger.error(taskType);
                            this.logger.error(JSON.stringify(params, (_, v) => typeof v === 'bigint' ? v.toString() : v));
                            this.logger.error(err.message);
                            this.logger.error(err.stack);
                            throw err;
                        })
                );
        };
        const blockBloom = new Bloom();
        for (const action of block.actions) {
            const aDuplicate = actions.find(other => {
                return other.receipt.act_digest === action.receipt.act_digest
            })
            if (aDuplicate)
                continue;

            if (!contractWhitelist.includes(action.act.account) ||
                !actionWhitelist.includes(action.act.name))
                continue;

            const isEvmContract = action.act.account === 'eosio.evm';
            const isRaw = isEvmContract && action.act.name === 'raw';
            const isWithdraw = isEvmContract && action.act.name === 'withdraw';
            const isDoResources = isEvmContract && action.act.name === 'doresources';
            const isOpenWallet = isEvmContract && action.act.name === 'openwallet';
            const isCreate = isEvmContract && action.act.name === 'create';
            const isSetRevision = isEvmContract && action.act.name === 'setrevision';

            const isTokenContract = action.act.account === 'eosio.token';
            const isTransfer = isTokenContract && action.act.name === 'transfer';

            const actData: any = action.act.data;

            const isDeposit = isTransfer && actData.to === 'eosio.evm';

            // discard transfers to accounts other than eosio.evm
            // and transfers from system accounts
            if ((isTransfer && action.receiver != 'eosio.evm') ||
                (isTransfer && actData.from in systemAccounts))
                continue;

            const params: HandlerArguments = {
                nativeBlockHash: block.blockInfo.this_block.block_id,
                trx_index: txTasks.length,
                blockNum: BigInt(currentEvmBlock),
                tx: actData,
                consoleLog: action.console
            };

            if (isDoResources) {
                const resChangeDelta = configChanges.shift();
                gasPriceEvents.push([evmTxIndex, resChangeDelta.gas_price]);
            } else if (isSetRevision)
                revisionEvents.push([evmTxIndex, actData.new_revision]);

            else if (isOpenWallet)
                openWalletEvents.push([evmTxIndex, actData.account, actData.address]);

            else if (isCreate)
                createEvents.push([evmTxIndex, actData.account, actData.data]);

            else if (isRaw)
                startTxTask('createEvm', params);

            else if (isWithdraw)
                startTxTask('createWithdraw', params);

            else if (isDeposit)
                startTxTask('createDeposit', params);

            else
                continue;

            actions.push(action);
            evmTxIndex++;
        }

        evmTxs = await Promise.all(txTasks);

        const evmTransactions: IndexedTx[] = []

        let gasUsedBlock = BigInt(0);
        let i = 0;
        for (const evmTx of evmTxs) {
            if (isTxDeserializationError(evmTx)) {
                this.logger.error('ds workerpool error:')
                this.logger.error(evmTx.message);
                this.logger.error(evmTx.stack);
                this.logger.error('evmTx ds error info');
                const errInfo = evmTx.info;
                this.logger.error(`block_num: ${errInfo.block_num}`);
                throw evmTx;
            }

            const action = actions[i];

            evmTx.trxId = action.trxId;
            evmTx.actionOrdinal = action.actionOrdinal;

            gasUsedBlock += BigInt(evmTx.gasUsed);
            evmTx.gasUsedBlock = gasUsedBlock;

            blockBloom.or(new Bloom(evmTx.logsBloom));

            evmTransactions.push(evmTx);
            i++;
        }

        const indexedBlock = IndexedBlockSchema.parse({
            timestamp: blockTimestamp,

            blockNum: currentBlock,
            blockHash: block.blockInfo.this_block.block_id,

            evmBlockNum: currentEvmBlock,
            evmBlockHash: ZERO_HASH,
            evmPrevHash: prevHash,

            receiptsRoot: new Uint8Array(),
            transactionsRoot: new Uint8Array(),

            gasUsed: gasUsedBlock,
            gasLimit: BLOCK_GAS_LIMIT,

            size: 0n,

            transactionAmount: evmTransactions.length,
            transactions: evmTransactions,
            gasPriceEvents,
            revisionEvents,
            openWalletEvents,
            createEvents,

            logsBloom: blockBloom.bitvector,

            deltas: {
                account: accountDeltas,
                accountstate: stateDeltas
            }
        });

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // fork handling
        if (currentBlock < this.lastBlock + 1n)
            indexedBlock.evmPrevHash = await this.handleFork(indexedBlock);

        const {
            evmBlockHash,
            receiptsRoot,
            txsRoot
        } = await this.hashBlock(indexedBlock);

        indexedBlock.evmBlockHash = evmBlockHash;
        indexedBlock.receiptsRoot = receiptsRoot;
        indexedBlock.transactionsRoot = txsRoot;

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // Update block num state tracking attributes
        this.lastBlock = currentBlock;

        // Push to db
        await this.targetConnector.pushBlock(indexedBlock);
        this.events.emit('push-block', indexedBlock);

        if (currentBlock == BigInt(this.chain.stopBlock)) {
            await this.stop();
            this.events.emit('stop');
            return;
        }

        // For debug stats
        this.pushedLastUpdate++;

        // If we are on EBR mode check for switch near head
        if (this._isReaderOnEBRMode && this.config.connector.chain.deployBlock - this.lastBlock <= 250000) {
            this.logger.info(`switching reader from EBR mode...`);
            this._isReaderOnEBRMode = false;
            // @ts-ignore
            this.reader.fetchDeltas = true;
            // @ts-ignore
            this.reader.fetchTraces = true;
            await this.resetReader();
        }

        return indexedBlock;
    }

    async getOldHash(blockNum: bigint) {
        const block = await this.targetConnector.getIndexedBlock(blockNum);
        if(!block)
            throw new Error(`Block #${blockNum} not found in db`);
        return block.evmBlockHash;
    }

    async startReaderFrom(prevHash: string, blockNum: bigint) {
        if (!this.config.connector.nodeos)
            throw new Error('Tried to start reader but no nodeos config provided');

        if (prevHash.length == 0)
            prevHash = ZERO_HASH;

        const nodeos = this.config.connector.nodeos;

        this._isReaderOnEBRMode = blockNum < this.config.connector.chain.deployBlock - BigInt(100000);

        this.reader = new HyperionSequentialReader({
            shipApi: nodeos.wsEndpoint,
            chainApi: nodeos.endpoint,
            poolSize: nodeos.readerWorkerAmount,
            blockConcurrency: nodeos.readerWorkerAmount,
            blockHistorySize: nodeos.blockHistorySize,
            startBlock: Number(blockNum),
            endBlock: Number(this.chain.stopBlock),
            actionWhitelist: {
                'eosio.token': ['transfer'],
                'eosio.msig': ['exec'],
                'eosio.evm': ['raw', 'withdraw', "doresources", "openwallet", "create", "setrevision"]
            },
            tableWhitelist: {
                'eosio.evm': ['account', 'accountstate', 'config']
            },
            irreversibleOnly: this.chain.irreversibleOnly,
            logLevel: (this.config.readerLogLevel || 'info').toLowerCase(),
            maxMsgsInFlight: this._isReaderOnEBRMode ? nodeos.maxMessagesInFlightEBR : nodeos.maxMessagesInFlight,
            maxPayloadMb: Math.floor(nodeos.maxWsPayloadMb),
            skipInitialBlockCheck: true,
            fetchTraces: !this._isReaderOnEBRMode,
            fetchDeltas: !this._isReaderOnEBRMode
        });

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

        this.reader.events.on('block', async (decodedBlock) => {
            const indexedBlock = await this.processSHIPBlock(prevHash, decodedBlock);
            prevHash = indexedBlock.evmBlockHash;
            this.reader.ack();
        });
        await this.reader.start();
    }

    newConnector(connConfig: any): Connector {
        return new ArrowConnector(connConfig);
    }

    /*
     * Entry point
     */
    async launch() {
        this.printIntroText();

        let startBlock = this.chain.startBlock;
        let prevHash: string;

        this.targetConnector = this.newConnector(this.config.connector);

        const gap = await this.targetConnector.init();

        if (this.config.runtime.onlyDBCheck) {
            this.logger.info('--only-db-check passed exiting...');
            await this.targetConnector.deinit();
            return;
        }

        let lastBlock = await this.targetConnector.getLastIndexedBlock();

        if (lastBlock != null &&
            lastBlock.evmPrevHash !== ZERO_HASH) {
            // if there is a last block found on db other than genesis doc

            if (gap == null) {
                ({startBlock, prevHash} = await this.getBlockInfoFromLastBlock(lastBlock));
            } else {
                if (this.config.connector.gapsPurge)
                    ({startBlock, prevHash} = await this.getBlockInfoFromGap(gap));
                else
                    throw new Error(
                        `Gap found in database at ${gap}, but --gaps-purge flag not passed!`);
            }

            this.chain.evmValidateHash = '';

        } else if (
            this.chain.evmPrevHash.length > 0 &&
            this.chain.evmPrevHash !== ZERO_HASH) {
            // if there is an evmPrevHash set state directly
            prevHash = this.chain.evmPrevHash;
        }

        if (prevHash)
            this.logger.info(`start from ${startBlock} with parent hash 0x${prevHash}.`);
        else {
            this.logger.info(`starting from genesis block ${startBlock}`);
            const [genesisHash, genesisBlockNum] = await this.genesisBlockInitialization();
            prevHash = genesisHash;
            startBlock = genesisBlockNum + 1n;
        }

        this.chain.startBlock = startBlock;
        this.lastBlock = startBlock - 1n;
        this.targetConnector.lastPushed = this.lastBlock;
        this.chain.evmPrevHash = prevHash;

        if (this.config.connector.nodeos) {
            const nodeosConfig = this.config.connector.nodeos;
            if (!nodeosConfig.skipStartBlockCheck) {
                // check node actually contains first block
                try {
                    await this.rpc.v1.chain.get_block(Number(startBlock));
                } catch (error) {
                    throw new Error(
                        `Error when doing start block check: ${error.message}`);
                }
            }

            if (!nodeosConfig.skipRemoteCheck) {
                // check remote node is up
                try {
                    await this.remoteRpc.v1.chain.get_info();
                } catch (error) {
                    this.logger.error(`Error while doing remote node check: ${error.message}`);
                    throw error;
                }
            }

            process.env.CHAIN_ID = this.chain.chainId.toString();
            process.env.ENDPOINT = this.config.connector.nodeos.endpoint;
            process.env.LOG_LEVEL = this.config.logLevel;

            this.evmDeserializationPool = workerpool.pool(
                './build/workers/handlers.js', {
                    minWorkers: this.config.connector.nodeos.evmWorkerAmount,
                    maxWorkers: this.config.connector.nodeos.evmWorkerAmount,
                    workerType: 'thread'
                });
        }

        await this.startReaderFrom(prevHash, BigInt(startBlock));

        // Launch bg routines
        this.perfTaskId = setInterval(async () => await this.performanceMetricsTask(), 1000);
        this.stateSwitchTaskId = setInterval(() => this.handleStateSwitch(), 10 * 1000);
    }

    async genesisBlockInitialization(): Promise<[string, bigint]> {
        const genesisBlock = await this.getGenesisBlock();

        // number of seconds since epoch
        const genesisTimestampSeconds = Math.floor(genesisBlock.timestamp.value.toNumber() / 1000);
        const genesisTimestamp = moment.utc(genesisTimestampSeconds).unix();

        // genesis evm block num
        const genesisEvmBlockNum = BigInt(genesisBlock.block_num.value.toNumber()) - this.chain.evmBlockDelta;
        const genesisHeader = TEVMBlockHeader.fromHeaderData({
            'number': BigInt(genesisEvmBlockNum),
            'stateRoot': EMPTY_TRIE_BUF,
            'gasLimit': BLOCK_GAS_LIMIT,
            'timestamp': BigInt(genesisTimestamp),
            'extraData': genesisBlock.id.array
        }, {common: this.common});

        const genesisHash = Buffer.from(genesisHeader.hash()).toString('hex');

        this.logger.info('ethereum genesis header: ');
        this.logger.info(JSON.stringify(genesisHeader.toJSON(), null, 4));

        this.logger.info(`ethereum genesis hash: 0x${genesisHash}`);

        if (this.chain.evmValidateHash.length > 0 &&
            this.chain.evmValidateHash !== ZERO_HASH) {
            if (genesisHash !== this.chain.evmValidateHash) {
                this.logger.error(`Generated genesis: \n${JSON.stringify(genesisHeader, null, 4)}`);
                throw new Error('FATAL!: Generated genesis hash doesn\'t match remote!');
            } else {
                // genesis hash validated, clean up chain.evmValidateHash to avoid double check on this.hashBlock
                this.chain.evmValidateHash = '';
                this.logger.info(`Validated genesis acording to config!`);
            }
        }

        // Init state tracking attributes
        const genesisBlockNum = BigInt(genesisBlock.block_num.value.toNumber());

        // if we are starting from genesis store block skeleton doc
        // for rpc to be able to find parent hash for fist block
        await this.targetConnector.pushBlock({
            timestamp: BigInt(genesisTimestamp),

            blockNum: genesisBlockNum,
            blockHash: genesisBlock.id.array,

            evmBlockNum: genesisEvmBlockNum,
            evmBlockHash: genesisHash,
            evmPrevHash: ZERO_HASH,

            receiptsRoot: EMPTY_TRIE,
            transactionsRoot: EMPTY_TRIE,

            gasUsed: 0n,
            gasLimit: BLOCK_GAS_LIMIT,

            size: 0n,

            transactionAmount: 0,

            transactions: [],

            gasPriceEvents: [],
            revisionEvents: [],
            openWalletEvents: [],
            createEvents: [],

            logsBloom: (new Bloom()).bitvector,

            deltas: {
                account: [],
                accountstate: []
            }
        })

        this.events.emit('start');

        return [genesisHash, genesisBlockNum];
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

        if (this.targetConnector) {
            try {
                await this.targetConnector.deinit();
            } catch (e) {
                this.logger.warn(`error stopping connector: ${e.message}`);
            }
        }

        if (this.sourceConnector) {
            try {
                await this.sourceConnector.deinit();
            } catch (e) {
                this.logger.warn(`error stopping connector: ${e.message}`);
            }
        }

        if (this.evmDeserializationPool) {
            try {
                await this.evmDeserializationPool.terminate(true);
            } catch (e) {
                this.logger.warn(`error stopping thread pool: ${e.message}`);
            }
        }

        // if (process.env.LOG_LEVEL == 'debug')
        //     setTimeout(logWhyIsNodeRunning, 5000);
    }

    /*
     * Poll remote rpc for genesis block, which is block previous to evm deployment
     */
    private async getGenesisBlock() {
        let genesisBlock = null;
        const retries = 10;
        let i = 0;
        while (i++ < retries) {
            try {
                // get genesis information
                genesisBlock = await this.rpc.v1.chain.get_block(
                    Number(this.chain.startBlock - 1n));
                break;

            } catch (e) {
                if (i < retries) {
                    this.logger.error(e);
                    this.logger.warn(`couldn\'t get genesis block ${(this.chain.startBlock - 1n).toLocaleString()}, attempt #${i}, retrying in 5 sec...`);
                    await sleep(5000);
                }
                this.logger.error(`Failed ${retries} times getting the genesis block, abort...`);
                throw e;
            }
        }

        return genesisBlock;
    }

    /*
     * Get start parameters from last block indexed on db
     */
    private async getBlockInfoFromLastBlock(lastBlock: IndexedBlock): Promise<StartBlockInfo> {

        // sleep, then get last block again, if block_num changes it means
        // another indexer is running
        await sleep(3000);
        const newlastBlock = await this.targetConnector.getLastIndexedBlock();
        if (lastBlock.blockNum != newlastBlock.blockNum)
            throw new Error(
                'New last block check failed probably another indexer is running, abort...');

        let startBlock = lastBlock.blockNum + 1n;

        this.logger.info('done.');

        lastBlock = await this.targetConnector.getLastIndexedBlock();

        const prettyTS = moment.utc(Number(lastBlock.timestamp)).toISOString();
        const prevHash = lastBlock.evmBlockHash;

        this.logger.info(
            `found! ${lastBlock.blockNum.toLocaleString()} produced on ${prettyTS} with hash 0x${prevHash}`);

        return {startBlock, prevHash};
    }

    /*
     * Get start parameters from first gap on database
     */
    private async getBlockInfoFromGap(gap: bigint): Promise<StartBlockInfo> {

        let firstBlock: IndexedBlock;
        let delta = 0n;
        while (!firstBlock || firstBlock.blockNum === undefined) {
            firstBlock = await this.targetConnector.getIndexedBlock(gap - delta);
            delta++;
        }
        // found blocks on the database
        this.logger.info(`Last block of continuous range found: ${JSON.stringify(firstBlock, null, 4)}`);

        let startBlock = firstBlock.blockNum;

        this.logger.info(`purge blocks newer than ${startBlock}`);

        await this.targetConnector.purgeNewerThan(startBlock);

        this.logger.info('done.');

        const lastBlock = await this.targetConnector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        if (lastBlock.blockNum != (startBlock - 1n))
            throw new Error(`Last block: ${lastBlock.blockNum.toLocaleString()}, is not ${(startBlock - 1n).toLocaleString()} - 1`);

        this.logger.info(
            `found! ${lastBlock} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return {startBlock, prevHash};
    }

    /*
     * Handle fork, leave every state tracking attribute in a healthy state
     */
    private async handleFork(b: IndexedBlock): Promise<string> {
        const lastNonForked = b.blockNum - 1n;
        const forkedAt = this.lastBlock;

        this.logger.info(`got ${b.blockNum.toLocaleString()} and expected ${(this.lastBlock + 1n).toLocaleString()}, chain fork detected. reverse all blocks which were affected`);

        await this.targetConnector.flush();

        // finally purge db
        await this.targetConnector.purgeNewerThan(lastNonForked + 1n);
        this.logger.debug(`purged db of blocks newer than ${lastNonForked}, continue...`);

        // tweak variables used by ordering machinery
        const newPrevHash = await this.getOldHash(lastNonForked);
        this.lastBlock = lastNonForked;

        this.targetConnector.forkCleanup(
            b.timestamp,
            lastNonForked,
            forkedAt
        );

        return newPrevHash;
    }

    printIntroText() {
        this.logger.info(`Telos EVM Translator v${packageInfo.version}`);
        this.logger.info('Happy indexing!');
    }
}

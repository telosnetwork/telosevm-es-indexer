import { StorageEosioAction, StorageEosioDelta } from './evm.js';

import { z } from 'zod';

// ConnectorConfig Schema
export const ConnectorConfigSchema = z.object({
    node: z.string().default('http://127.0.0.1:9200'),
    requestTimeout: z.number().default(5000),
    docsPerIndex: z.number().default(10000000),
    scrollSize: z.number().default(6000),
    scrollWindow: z.string().default('8s'),
    numberOfShards: z.number().default(1),
    numberOfReplicas: z.number().default(0),
    refreshInterval: z.number().default(-1),
    storeRaw: z.boolean().default(false),
    codec: z.string().default('best_compression'),
    suffix: z
        .object({
            delta: z.string().default('delta-v1.5'),
            transaction: z.string().default('action-v1.5'),
            fork: z.string().default('fork-v1.5'),
        })
        .default({
            delta: 'delta-v1.5',
            transaction: 'action-v1.5',
            fork: 'fork-v1.5',
        }),
});

export const BroadcasterConfigSchema = z
    .object({
        wsHost: z.string().default('127.0.0.1'),
        wsPort: z.number().default(7300),
    })
    .default({ wsHost: '127.0.0.1', wsPort: 7300 });

export const TranslatorConfigSchema = z.object({
    logLevel: z.string().default('debug'),
    readerLogLevel: z.string().default('info'),
    chainName: z.string(),
    chainId: z.number(),
    runtime: z
        .object({
            trimFrom: z.number().optional(),
            skipIntegrityCheck: z.boolean().optional(),
            onlyDBCheck: z.boolean().optional(),
            gapsPurge: z.boolean().optional(),
            skipStartBlockCheck: z.boolean().optional(),
            skipRemoteCheck: z.boolean().optional(),
            reindexInto: z.string().optional(),
        })
        .default({}),
    endpoint: z.string().default('http://127.0.0.1:8888'),
    remoteEndpoint: z.string().default('http://127.0.0.1:8888'),
    wsEndpoint: z.string().default('ws://127.0.0.1:29999'),
    evmBlockDelta: z.number().default(0),
    evmPrevHash: z.string().default(''),
    evmValidateHash: z.string().default(''),
    startBlock: z.number(),
    stopBlock: z.number().default(-1),
    irreversibleOnly: z.boolean().default(false),
    perf: z
        .object({
            stallCounter: z.number().default(5),
            readerWorkerAmount: z.number().default(4),
            evmWorkerAmount: z.number().default(4),
            elasticDumpSize: z.number().default(2 * 1000),
            maxMessagesInFlight: z.number().default(32),
        })
        .default({}),
    elastic: ConnectorConfigSchema,
    broadcast: BroadcasterConfigSchema,
});

export type BroadcasterConfig = z.infer<typeof BroadcasterConfigSchema>;
export type TranslatorConfig = z.infer<typeof TranslatorConfigSchema>;

export type IndexedBlockInfo = {
    transactions: StorageEosioAction[];
    delta: StorageEosioDelta;
    nativeHash: string;
    parentHash: string;
    receiptsRoot: string;
    blockBloom: string;
};

export enum IndexerState {
    SYNC = 0,
    HEAD = 1,
}

export type StartBlockInfo = {
    startBlock: number;
    startEvmBlock?: number;
    prevHash: string;
};

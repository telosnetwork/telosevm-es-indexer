// relevant native action parameter type specs
import {arrayToHex} from "../utils/evm.js";

export interface EosioEvmRaw {
    ram_payer: string,
    tx: string,
    estimate_gas: boolean,
    sender: null | string
}

export interface EosioEvmDeposit {
    from: string,
    to: string,
    quantity: string,
    memo: string
}

export interface EosioEvmWithdraw {
    to: string,
    quantity: string
}

// generic representations of data as it goes through the translator from source
// connector to target.

import { z } from 'zod';

// Helper function to transform base64 or hex strings into Uint8Array
const stringToUint8Array = (input: string): Uint8Array => {
    if (input.startsWith('0x')) {
        input = input.slice(2);
    }
    if (/^[0-9a-fA-F]+$/.test(input)) {
        const hex = input.length % 2 ? '0' + input : input;
        return new Uint8Array(hex.match(/.{1,2}/g)!.map(byte => parseInt(byte, 16)));
    }
    return new Uint8Array(Buffer.from(input, 'base64'));
};

// Custom Zod type for Uint8Array transformation
const uint8ArraySchema = z.union([z.string(), z.instanceof(Uint8Array)]).transform((val) => {
    if (typeof val === 'string') {
        return stringToUint8Array(val);
    }
    return val;
})

// Helper function to transform base64 or hex strings into unprefixed hex string
const toUnprefixedHexString = (input: string | Uint8Array): string => {
    if (input instanceof Uint8Array) {
        return arrayToHex(input);
    }
    if (input.startsWith('0x')) {
        input = input.slice(2);
    }
    if (/^[0-9a-fA-F]+$/.test(input)) {
        const hex = input.length % 2 ? '0' + input : input;
        return hex.toLowerCase();
    }
    const uint8Array = new Uint8Array(Buffer.from(input, 'base64'));
    return Array.from(uint8Array).map(byte => byte.toString(16).padStart(2, '0')).join('');
};

// Custom Zod type for checksum transformation
const checksumSchema = z.union([z.string(), z.instanceof(Uint8Array)]).transform((val) => {
    return toUnprefixedHexString(val);
});

// IndexedInternalTx schema
export const IndexedInternalTxSchema = z.object({
    callType: z.string(),
    from: z.string(),
    gas: z.bigint(),
    input: z.string(),
    to: z.string(),
    value: z.bigint(),
    gasUsed: z.bigint(),
    output: z.string(),
    subTraces: z.string(),
    traceAddress: z.array(z.string()),
    type: z.string(),
    depth: z.string(),
});

// IndexedTxLog schema
export const IndexedTxLogSchema = z.object({
    address: z.string().optional(),
    topics: z.array(z.string()).optional(),
    data: z.string().optional(),
});

// IndexedTx schema
export const IndexedTxSchema = z.object({
    trxId: checksumSchema,
    trxIndex: z.number(),
    actionOrdinal: z.number(),
    blockNum: z.bigint(),
    blockHash: checksumSchema,
    hash: checksumSchema,
    raw: uint8ArraySchema,
    from: checksumSchema.optional(),
    to: checksumSchema.optional(),
    inputData: uint8ArraySchema,
    value: z.bigint(),
    nonce: z.bigint(),
    gasPrice: z.bigint(),
    gasLimit: z.bigint(),
    v: z.bigint(),
    r: z.bigint(),
    s: z.bigint(),
    status: z.union([z.literal(0), z.literal(1)]),
    itxs: z.array(IndexedInternalTxSchema).default([]),
    epoch: z.number(),
    createAddr: checksumSchema.optional(),
    gasUsed: z.bigint(),
    gasUsedBlock: z.bigint(),
    chargedGasPrice: z.bigint(),
    output: uint8ArraySchema,
    logs: z.array(IndexedTxLogSchema).default([]),
    logsBloom: uint8ArraySchema,
    errors: z.array(z.string()).default([]),
});

// IndexedAccountDelta schema
export const IndexedAccountDeltaSchema = z.object({
    timestamp: z.bigint(),
    blockNum: z.bigint(),
    ordinal: z.number(),
    index: z.number(),
    address: z.string(),
    account: z.string(),
    nonce: z.number(),
    code: uint8ArraySchema,
    balance: z.string(),
});

// IndexedAccountStateDelta schema
export const IndexedAccountStateDeltaSchema = z.object({
    timestamp: z.bigint(),
    blockNum: z.bigint(),
    ordinal: z.number(),
    scope: z.string(),
    index: z.number(),
    key: z.string(),
    value: z.string(),
});

export const IndexedConfigDeltaSchema = z.object({
    timestamp: z.bigint(),
    blockNum: z.bigint(),
    ordinal: z.number(),
    trx_index: z.number(),
    last_block: z.number(),
    gas_used_block: z.string(),
    gas_price: z.string()
});

// IndexedBlockHeader schema
export const IndexedBlockHeaderSchema = z.object({
    timestamp: z.bigint(),
    blockNum: z.bigint(),
    blockHash: checksumSchema,
    evmBlockNum: z.bigint(),
    evmBlockHash: checksumSchema,
    evmPrevHash: checksumSchema,
    receiptsRoot: checksumSchema,
    transactionsRoot: checksumSchema,
    gasUsed: z.bigint(),
    gasLimit: z.bigint(),
    size: z.bigint(),
    transactionAmount: z.number(),
});

// IndexedBlock schema
export const IndexedBlockSchema = IndexedBlockHeaderSchema.extend({
    transactions: z.array(IndexedTxSchema),
    gasPriceEvents: z.array(z.tuple([z.number(), z.string()])),
    revisionEvents: z.array(z.tuple([z.number(), z.number()])),
    openWalletEvents: z.array(z.tuple([z.number(), z.string(), z.string()])),
    createEvents: z.array(z.tuple([z.number(), z.string(), z.string()])),
    logsBloom: uint8ArraySchema,
    deltas: z.object({
        account: z.array(IndexedAccountDeltaSchema),
        accountstate: z.array(IndexedAccountStateDeltaSchema),
    }),
});

// Infer types
export type IndexedInternalTx = z.infer<typeof IndexedInternalTxSchema>;
export type IndexedTxLog = z.infer<typeof IndexedTxLogSchema>;
export type IndexedTx = z.infer<typeof IndexedTxSchema>;
export type IndexedAccountDelta = z.infer<typeof IndexedAccountDeltaSchema>;
export type IndexedAccountStateDelta = z.infer<typeof IndexedAccountStateDeltaSchema>;
export type IndexedConfigDelta = z.infer<typeof IndexedConfigDeltaSchema>;
export type IndexedBlockHeader = z.infer<typeof IndexedBlockHeaderSchema>;
export type IndexedBlock = z.infer<typeof IndexedBlockSchema>;

export enum IndexerState {
    SYNC = 0,
    HEAD = 1
}

export type StartBlockInfo = {
    startBlock: bigint;
    startEvmBlock?: bigint;
    prevHash: string;
}
import {
    EosioEvmRaw,
    StorageEvmTransaction
} from '../types/evm';

// ethereum tools
import {Bloom, generateUniqueVRS} from '../utils/evm';
import {TEVMTransaction} from '../utils/evm-tx';
import Common from '@ethereumjs/common'
import { Chain, Hardfork } from '@ethereumjs/common'

const BN = require('bn.js');

import { parentPort, workerData } from 'worker_threads';

import logger from '../utils/winston';
import { isHexPrefixed, isValidAddress } from '@ethereumjs/util';

const args: {chainId: number} = workerData;

logger.info('Launching evm tx deserialization worker...');

const common: Common = Common.custom({
    chainId: args.chainId,
    defaultHardfork: Hardfork.Istanbul
}, {
    baseChain: Chain.Mainnet
});

const KEYWORD_STRING_TRIM_SIZE = 32000;
const RECEIPT_LOG_START = "RCPT{{";
const RECEIPT_LOG_END = "}}RCPT";

parentPort.on(
    'message',
    (param: Array<{
        nativeBlockHash: string,
        trx_index: number,
        blockNum: number,
        tx: EosioEvmRaw,
        consoleLog: string
    }>) => {
        const arg = param[0];
        try {
            let receiptLog = arg.consoleLog.slice(
                arg.consoleLog.indexOf(RECEIPT_LOG_START) + RECEIPT_LOG_START.length,
                arg.consoleLog.indexOf(RECEIPT_LOG_END)
            );

            let receipt: {
                status: number,
                epoch: number,
                itxs: any[],
                logs: any[],
                errors?: any[],
                output: string,
                gasused: string,
                gasusedblock: string,
                charged_gas: string,
                createdaddr: string
            } = {
                status: 1,
                epoch: 0,
                itxs: [],
                logs: [],
                errors: [],
                output: '',
                gasused: '',
                gasusedblock: '',
                charged_gas: '',
                createdaddr: ''
            };
            try {
                receipt = JSON.parse(receiptLog);
                // logger.info(`Receipt: ${JSON.stringify(receipt)}`);
            } catch (e) {
                logger.warn('Failed to parse receiptLog');
            }

            // disable this check due to the posibility of onblock failing
            // if (receipt.block != arg.blockNum)
            //    throw new Error("Block number mismach");

            const txRaw = Buffer.from(arg.tx.tx, 'hex');
           
            let evmTx = TEVMTransaction.fromSerializedTx(
                txRaw, {common});

            const evmTxParams = evmTx.toJSON();
            let fromAddr = null;

            if (arg.tx.sender != null) {

                let senderAddr = arg.tx.sender.toLowerCase();

                if (!isHexPrefixed(senderAddr))
                    senderAddr = `0x${senderAddr}`;

                const [v, r, s] = generateUniqueVRS(
                    arg.nativeBlockHash, senderAddr, arg.trx_index);

                evmTxParams.v = v;
                evmTxParams.r = r;
                evmTxParams.s = s;

                evmTx = TEVMTransaction.fromTxData(evmTxParams, {common});

                if(isValidAddress(senderAddr)) {
                    fromAddr = senderAddr;

                } else {
                    logger.error(`error deserializing address \'${arg.tx.sender}\'`);
                    return parentPort.postMessage({success: false, message: 'invalid address'});
                }
            }

            if (receipt.itxs) {
                // @ts-ignore
                receipt.itxs.forEach((itx) => {
                    if (itx.input)
                        itx.input_trimmed = itx.input.substring(0, KEYWORD_STRING_TRIM_SIZE);
                    else
                        itx.input_trimmed = itx.input;
                });
            }

            const txBody: StorageEvmTransaction = {
                hash: '0x'+ evmTx.hash().toString('hex'),
                trx_index: arg.trx_index,
                block: arg.blockNum,
                block_hash: "",
                to: evmTx.to?.toString(),
                input_data: evmTx.data?.toString('hex'),
                input_trimmed: evmTx.data?.toString('hex').substring(0, KEYWORD_STRING_TRIM_SIZE),
                value: evmTx.value?.toString('hex'),
                value_d: new BN(evmTx.value?.toString()) / new BN('1000000000000000000'),
                nonce: evmTx.nonce?.toString(),
                gas_price: evmTx.gasPrice?.toString(),
                gas_limit: evmTx.gasLimit?.toString(),
                status: receipt.status,
                itxs: receipt.itxs,
                epoch: receipt.epoch,
                createdaddr: receipt.createdaddr.toLowerCase(),
                gasused: parseInt('0x' + receipt.gasused),
                gasusedblock: parseInt('0x' + receipt.gasusedblock),
                charged_gas_price: parseInt('0x' + receipt.charged_gas),
                output: receipt.output,
                raw: txRaw 
            };

            if (fromAddr != null)
                txBody['from'] = fromAddr;

            else
                txBody['from'] = evmTx.getSenderAddress().toString().toLowerCase();

            if (receipt.logs) {
                txBody.logs = receipt.logs;
                if (txBody.logs.length === 0) {
                    delete txBody['logs'];
                } else {
                    //console.log('------- LOGS -----------');
                    //console.log(txBody['logs']);
                    const bloom = new Bloom();
                    for (const log of txBody['logs']) {
                        bloom.add(Buffer.from(log['address'], 'hex'));
                        for (const topic of log.topics)
                            bloom.add(Buffer.from(topic.padStart(64, '0'), 'hex'));
                    }

                    txBody['logsBloom'] = bloom.bitvector.toString('hex');
                }
            }

            if (receipt.errors) {
                txBody['errors'] = receipt.errors;
                if (txBody['errors'].length === 0) {
                    delete txBody['errors'];
                } else {
                    //console.log('------- ERRORS -----------');
                    //console.log(txBody['errors'])
                }
            }

            return parentPort.postMessage({success: true, tx: txBody});
        } catch (e) {
            return parentPort.postMessage({success: false, message: {
                'stack': e.stack,
                'name': e.name,
                'message': e.message
            }});
        }
    }
);

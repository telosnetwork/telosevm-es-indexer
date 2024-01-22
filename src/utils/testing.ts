import toxiproxyClient, {ICreateToxicBody, Latency} from "toxiproxy-node-client";
import {Client} from "@elastic/elasticsearch";
import {readFileSync} from "node:fs";
import path from "node:path";
import {JsonRpc} from "eosjs";
import {clearInterval} from "timers";
import {CONFIG_TEMPLATES_DIR, sleep, TEST_RESOURCES_DIR} from "./indexer.js";
import {decompressFile, maybeFetchResource, maybeLoadElasticDump} from "./resources.js";
import {TEVMIndexer} from "../indexer.js";
import {initializeNodeos} from "./nodeos.js";
import moment from "moment";


export async function getElasticDeltas(esclient, index, from, to) {
    try {
        const hits = [];
        let result = await esclient.search({
            index: index,
            query: {
                range: {
                    block_num: {
                        gte: from,
                        lte: to
                    }
                }
            },
            size: 1000,
            sort: [{ 'block_num': 'asc' }],
            scroll: '3s'
        });

        let scrollId = result._scroll_id;
        while (result.hits.hits.length) {
            result.hits.hits.forEach(hit => hits.push(hit._source));
            result = await esclient.scroll({
                scroll_id: scrollId,
                scroll: '3s'
            });
            scrollId = result._scroll_id;
        }
        await esclient.clearScroll({scroll_id: scrollId});

        return hits;
    } catch (e) {
        if (!e.message.includes('index_not_found')) {
            console.error(e.message);
            console.error(e.stack);
        }
        return [];
    }
}

export async function getElasticActions(esclient, index, from, to) {
    try {
        const hits = [];
        let result = await esclient.search({
            index: index,
            query: {
                range: {
                    '@raw.block': {
                        gte: from,
                        lte: to
                    }
                }
            },
            size: 1000,
            sort: [{ '@raw.block': 'asc' }, { '@raw.trx_index': 'asc' }],
            scroll: '3s' // Keep the search context alive for 1 minute
        });

        let scrollId = result._scroll_id;
        while (result.hits.hits.length) {
            result.hits.hits.forEach(hit => hits.push(hit._source));
            result = await esclient.scroll({
                scroll_id: scrollId,
                scroll: '3s'
            });
            scrollId = result._scroll_id;
        }
        await esclient.clearScroll({scroll_id: scrollId});

        return hits;
    } catch (e) {
        if (!e.message.includes('index_not_found')) {
            console.error(e.message);
            console.error(e.stack);
        }
        return [];
    }
}

export function chance(percent) {
    return Math.random() <= percent;
}

// toxiproxy-node-client is :(
export async function createShortLivedToxic(proxy, config, ms) {
    const toxic = new toxiproxyClient.Toxic(null, config);
    await proxy.addToxic(toxic);  // circular json error if toxic.proxy != null when doing addToxic
    await sleep(ms);
    toxic.proxy = proxy;
    await toxic.remove();
}


const latencyConfig = {
    name: 'latencyToxic',
    type: 'latency',
    attributes: {
        latency: 0,
        jitter: 500
    }
};

const timeoutConfig = {
    name: 'timeoutToxic',
    type: 'timeout',
    attributes: {
        timeout: 3000
    }
};

const resetPeerConfig = {
    name: 'resetToxic',
    type: 'reset_peer',
    attributes: {
        timeout: 0
    }
};

export async function toxiInitializeTestProxy(
    toxiClient, name: string, port: number, toxiPort: number,
    toxicConfig
) {
    const toxicProxyInfo = {
        name,
        listen: `127.0.0.1:${toxiPort}`,
        upstream: `127.0.0.1:${port}`
    };

    try {
        await(await toxiClient.get(name)).remove();

    } catch (e) {
        if (!('response' in e) ||
            !('data' in e.response) ||
            !('error' in e.response.data) ||
            e.response.data.error !== 'proxy not found')
            throw e;

    }

    const proxy = await toxiClient.createProxy(toxicProxyInfo);

    if (toxicConfig.latency) {
        const toxicBody = {
            ...latencyConfig,
            attributes: {...toxicConfig.latency},
            stream: 'downstream',
            toxicity: 1.0
        };
        // @ts-ignore
        await proxy.addToxic(new toxiproxyClient.Toxic(proxy, toxicBody));
    }

    return proxy;
}

export interface ESConfig {
    host: string;
    verification: {
        delta: string;
        action: string;
    };
    esDumpLimit?: number;
    purge?: boolean;
}

export interface ToxiConfig {
    toxics: {
        drops: boolean;
        latency?: ICreateToxicBody<Latency>;
    };
    host: string;
}

export interface ESVerificationTestParameters {
    title: string;
    timeout: number;
    resources: {
        type: string;
        url: string;
        destinationPath: string;
        decompressPath?: string;
    }[];
    elastic: ESConfig;
    toxi?: ToxiConfig;
    nodeos: {
        httpPort: number;
        toxiHttpPort: number;
        shipPort: number;
        toxiShipPort: number;
    }
    translator: {
        template: string;
        srcPrefix?: string;
        dstPrefix: string;
        startBlock: number;
        totalBlocks: number;
        evmPrevHash: string;
        evmValidateHash: string;
        stallCounter: number;
        indexVersion?: string;
        esDumpSize?: number;
        scrollWindow?: string;
        scrollSize?: number;
    }
}

export async function translatorESReplayVerificationTest(testParams: ESVerificationTestParameters) {
    const testTitle = testParams.title;
    const testTimeout = testParams.timeout;
    const testTitleSane = testTitle.split(' ').join('-').toLowerCase();
    const testStartTime = new Date().getTime();

    const defESConfig = {
        host: 'http://127.0.0.1:9200',
        esDumpLimit: 4000
    }
    const esConfig = {...defESConfig, ...testParams.elastic};
    const esClient = new Client({node: esConfig.host});
    try {
        await esClient.ping();
    } catch (e) {
        console.error('Could not ping elastic, is it up?');
        process.exit(1);
    }

    const defToxiConfig = {
        host: 'http://127.0.0.1:8474',
    };
    let toxiConfig: ToxiConfig;
    let toxiProxy;
    if (testParams.toxi) {
        toxiConfig = {...defToxiConfig, ...testParams.toxi};
        toxiProxy = new toxiproxyClient.Toxiproxy(toxiConfig.host);
        try {
            await toxiProxy.getVersion();
        } catch (e) {
            console.error('Could not get toxiproxy version, is it up?');
            process.exit(1);
        }
        await toxiProxy.reset();
    }

    const defNodeosConfig = {
        httpPort: 8888, toxiHttpPort: 8889,
        shipPort: 29999, toxiShipPort: 30000,
    };
    const nodeosConfig = {...defNodeosConfig, ...testParams.nodeos};
    const nodeosHttpHost = `http://127.0.0.1:${nodeosConfig.httpPort}`;
    const nodeosShipHost = `http://127.0.0.1:${nodeosConfig.shipPort}`;
    const toxiNodeosHttpHost = `http://127.0.0.1:${nodeosConfig.toxiHttpPort}`;
    const toxiNodeosShipHost = `http://127.0.0.1:${nodeosConfig.toxiShipPort}`;

    const defTranslatorConfig = {
        template: 'config.mainnet.json',
        startBlock: 180698860,
        totalBlocks: 10000,
        // evmPrevHash: '',
        stallCounter: 2
    };
    const translatorTestConfig = {...defTranslatorConfig, ...testParams.translator};
    const translatorConfig = JSON.parse(
        readFileSync(path.join(CONFIG_TEMPLATES_DIR, translatorTestConfig.template)).toString()
    );

    const startBlock = translatorTestConfig.startBlock;
    const endBlock = startBlock + translatorTestConfig.totalBlocks;

    translatorConfig.chainName = translatorTestConfig.dstPrefix;
    translatorConfig.endpoint = toxiConfig.toxics ? toxiNodeosHttpHost : nodeosHttpHost;
    translatorConfig.wsEndpoint = toxiConfig.toxics ? toxiNodeosShipHost : nodeosShipHost;
    translatorConfig.startBlock = startBlock;
    translatorConfig.stopBlock = endBlock;
    // translatorConfig.evmPrevHash = translatorTestConfig.evmPrevHash;
    translatorConfig.perf.stallCounter = translatorTestConfig.stallCounter;

    if (translatorTestConfig.totalBlocks < translatorConfig.perf.elasticDumpSize)
        translatorConfig.perf.elasticDumpSize = translatorTestConfig.totalBlocks;

    const adjustedNum = Math.floor(startBlock / translatorConfig.elastic.docsPerIndex);
    const numericIndexSuffix = String(adjustedNum).padStart(8, '0');
    const genDeltaIndexName = `${translatorConfig.chainName}-${translatorConfig.elastic.subfix.delta}-${numericIndexSuffix}`;
    const genActionIndexName = `${translatorConfig.chainName}-${translatorConfig.elastic.subfix.transaction}-${numericIndexSuffix}`;

    if (esConfig.purge) {
        // try to delete generated data in case it exists
        try {
            await esClient.indices.delete({index: genDeltaIndexName});
        } catch (e) {
        }
        try {
            await esClient.indices.delete({index: genActionIndexName});
        } catch (e) {
        }
    }

    // maybe download & decompress resources
    let nodeosSnapshotName = undefined;
    let nodeosSnapshotPath = undefined;
    const maybeInitializeResouce = async (res) => {
        const resName = await maybeFetchResource(res)
        if (res.type === 'esdump')
            await maybeLoadElasticDump(resName, esConfig);

        else if (res.type === 'snapshot') {
            nodeosSnapshotName = resName;
            nodeosSnapshotPath = res.decompressPath;

        } else if (res.type === 'nodeos') {
            if (!nodeosSnapshotName) {
                console.log('Nodeos launch pending until snapshot available...');
                while(!nodeosSnapshotName) await sleep(1000);
            }
            console.log(`Snapshot ${nodeosSnapshotName} found, maybe launching nodeos...`);

            try {
                const nodeosRpc = new JsonRpc(nodeosHttpHost);
                const chainInfo = await nodeosRpc.get_info();

                // @ts-ignore
                if (chainInfo.earliest_available_block_num > startBlock)
                    throw new Error(`Nodeos does not contain ${startBlock}`);
                if (chainInfo.head_block_num < endBlock)
                    throw new Error(`Nodeos does not contain ${endBlock}`);

                console.log(`Nodeos contains start & end block, skipping replay...`);

            } catch (e) {
                if (!e.message.includes('Could not find block') &&
                    !(e.message.includes('fetch failed')))
                    throw e;

                await initializeNodeos({
                    snapshot: nodeosSnapshotName,
                    dataDir: resName, runtimeDir: testTitleSane,
                    replay: !!res.replay
                });
            }
        }
    };
    if (testParams.resources)
        await Promise.all(testParams.resources.map(
            res => maybeInitializeResouce(res)
        ));

    let httpProxy = undefined;
    let shipProxy = undefined;
    let dropTask;
    let totalTimeouts = 0;
    let totalResets = 0;
    let totalDowntime = 0;
    if (toxiConfig.toxics) {
        httpProxy = await toxiInitializeTestProxy(
            toxiProxy, `${testTitleSane}-nodeos-http`,
            testParams.nodeos.httpPort,
            testParams.nodeos.toxiHttpPort,
            toxiConfig.toxics
        );
        shipProxy = await toxiInitializeTestProxy(
            toxiProxy, `${testTitleSane}-nodeos-ship`,
            testParams.nodeos.shipPort,
            testParams.nodeos.toxiShipPort,
            toxiConfig.toxics
        );

        if ('drops' in toxiConfig.toxics &&
            toxiConfig.toxics.drops) {
            let isDropTaskInprogress = false;
            setTimeout(() => {
                dropTask = setInterval(async () => {
                    if (!isDropTaskInprogress) {
                        isDropTaskInprogress = true;
                        const dropStartTime = new Date().getTime();

                        if (chance(1 / 20)) {
                            await createShortLivedToxic(httpProxy, resetPeerConfig, 50);
                            await createShortLivedToxic(shipProxy, resetPeerConfig, 50);
                            totalResets++;
                        } else {
                            if (chance(1 / 60)) {
                                await createShortLivedToxic(httpProxy, timeoutConfig, 6000);
                                await createShortLivedToxic(shipProxy, timeoutConfig, 6000);
                                totalTimeouts++;
                            }
                        }

                        const dropTimeElapsed = (new Date().getTime()) - dropStartTime;
                        totalDowntime += dropTimeElapsed;
                        isDropTaskInprogress = false;
                    }
                }, 1000);
            }, 1000);
        }
    }

    // launch translator and verify generated data
    let isTranslatorDone = false;
    const translator = new TEVMIndexer(translatorConfig);

    let esQueriesDone = 0;
    let esDocumentsChecked = 0;

    const translatorLaunchTime = new Date().getTime();
    let translatorSyncTime;
    await translator.launch();
    translator.connector.events.on('write', async (writeInfo) => {
        // // get all block documents in write range
        // const deltas = await getElasticDeltas(esClient, genDeltaIndexName, writeInfo.from, writeInfo.to);
        // const verifiedDeltas = await getElasticDeltas(esClient, esConfig.verification.delta, writeInfo.from, writeInfo.to);
        // assert(deltas.length === verifiedDeltas.length, 'getElasticDelta length difference');

        // deltas.forEach((delta, index) => {
        //     const verifiedDelta = StorageEosioDeltaSchema.parse(verifiedDeltas[index]);
        //     const parsedDelta = StorageEosioDeltaSchema.parse(delta);

        //     expect(parsedDelta.block_num).to.be.equal(verifiedDelta.block_num);
        //     expect(parsedDelta['@timestamp']).to.be.equal(verifiedDelta['@timestamp']);
        //     expect(parsedDelta['@blockHash']).to.be.equal(verifiedDelta['@blockHash']);

        //     let gasUsed = parsedDelta.gasUsed;
        //     if (!gasUsed)
        //         gasUsed = '0';
        //     expect(gasUsed).to.be.equal(verifiedDelta.gasUsed);
        //     esDocumentsChecked++;
        // });

        // // get all tx documents in write range
        // const actions = await getElasticActions(esClient, genActionIndexName, writeInfo.from, writeInfo.to);
        // esQueriesDone++;
        // const verifiedActions = await getElasticActions(esClient, esConfig.verification.action, writeInfo.from, writeInfo.to);
        // esQueriesDone++;
        // assert(actions.length === verifiedActions.length, 'getElasticActions length difference');

        // actions.forEach((action, index) => {
        //     const verifiedActionDoc = verifiedActions[index];
        //     const verifiedAction = StorageEosioActionSchema.parse(verifiedActionDoc);
        //     const parsedAction = StorageEosioActionSchema.parse(action);

        //     expect(parsedAction['@raw'].hash).to.be.equal(verifiedAction['@raw'].hash);
        //     expect(parsedAction['@raw'].gasused).to.be.equal(verifiedAction['@raw'].gasused);
        //     expect(parsedAction['@raw'].gasusedblock).to.be.equal(verifiedAction['@raw'].gasusedblock);
        //     esDocumentsChecked++;
        // });

        const now = new Date().getTime();

        const currentTestTimeElapsed = moment.duration(now - testStartTime, 'ms').humanize();
        const currentSyncTimeElapsed = moment.duration(now - translatorLaunchTime, 'ms').humanize();

        const checkedBlocksCount = writeInfo.to - startBlock;
        const progressPercent = (((checkedBlocksCount / translatorTestConfig.totalBlocks) * 100).toFixed(2) + '%').padStart(6, ' ');
        const currentProgress = (writeInfo.to - startBlock).toLocaleString();

        console.log('-'.repeat(32));
        console.log('Test stats:');
        console.log(`last checked range ${writeInfo.from.toLocaleString()} - ${writeInfo.to.toLocaleString()}`);
        console.log(`progress: ${progressPercent}, ${currentProgress} blocks`);
        console.log(`total test time: ${currentTestTimeElapsed}`);
        console.log(`total sync time: ${currentSyncTimeElapsed}`);
        console.log(`es queries done: ${esQueriesDone}`);
        console.log(`es docs checked: ${esDocumentsChecked}`);
        console.log('-'.repeat(32));

        if (writeInfo.to === endBlock) {
            isTranslatorDone = true;
            translatorSyncTime = new Date().getTime();
        }
    });

    while (!isTranslatorDone) {
        const now = new Date().getTime();
        if (((now - translatorSyncTime) / 1000) >= testTimeout)
            throw new Error('Translator sync timeout exceeded!');
        await sleep(200);
    }

    const totalTestTimeElapsed = translatorSyncTime - testStartTime;
    const translatorSyncTimeElapsed = translatorSyncTime - translatorLaunchTime;

    const posibleSyncTimeSeconds = (translatorSyncTimeElapsed - totalDowntime) / 1000;

    console.log('Test passed!');
    console.log(`total time elapsed: ${moment.duration(totalTestTimeElapsed, 'ms').humanize()}`);
    console.log(`sync time:          ${moment.duration(translatorSyncTimeElapsed, 'ms').humanize()}`);
    console.log(`down time:          ${moment.duration(totalDowntime, 'ms').humanize()}`);
    console.log(`es queries done:    ${esQueriesDone}`);
    console.log(`es docs checked:    ${esDocumentsChecked}`);
    if (toxiConfig.toxics) {
        if (toxiConfig.toxics.drops) {
            console.log(`# of reset_peer:    ${totalResets}`);
            console.log(`# of timeout:       ${totalTimeouts}`);
        }
        if (toxiConfig.toxics.latency) {
            console.log(`applied latency:    ${latencyConfig.attributes.latency}ms`);
            console.log(`applied jitter:     ${latencyConfig.attributes.jitter}ms`);
        }
    }
    console.log(`average speed:      ${(translatorTestConfig.totalBlocks / posibleSyncTimeSeconds).toFixed(2).toLocaleString()}`);

    if (dropTask)
        clearInterval(dropTask);


    if (toxiConfig.toxics) {
        if (httpProxy)
            await httpProxy.remove();

        if (shipProxy)
            await shipProxy.remove();
    }

    await esClient.close();
}


export async function translatorESReindexVerificationTest(
    testParams: ESVerificationTestParameters
) {
    if (!testParams.translator.srcPrefix)
        throw new Error('Reindex test requires srcPrefix config!');

    // const testTitle = testParams.title;
    // const testTitleSane = testTitle.split(' ').join('-').toLowerCase();
    const testTimeout = testParams.timeout;
    const testStartTime = performance.now();

    const defESConfig = {
        host: 'http://127.0.0.1:9200',
        esDumpLimit: 4000
    }
    const esConfig = {...defESConfig, ...testParams.elastic};
    const esClient = new Client({node: esConfig.host});
    try {
        await esClient.ping();
    } catch (e) {
        console.error('Could not ping elastic, is it up?');
        process.exit(1);
    }

    const translatorConfig = JSON.parse(
        readFileSync(path.join(CONFIG_TEMPLATES_DIR, testParams.translator.template)).toString()
    );

    const startBlock = testParams.translator.startBlock;
    const endBlock = startBlock + testParams.translator.totalBlocks;

    translatorConfig.chainName = testParams.translator.srcPrefix;
    translatorConfig.startBlock = startBlock;
    translatorConfig.stopBlock = endBlock;
    translatorConfig.evmPrevHash = testParams.translator.evmPrevHash;

    if (testParams.translator.evmValidateHash)
        translatorConfig.evmValidateHash = testParams.translator.evmValidateHash;
    else
        translatorConfig.evmValidateHash = '';

    if (testParams.translator.indexVersion) {
        translatorConfig.elastic.subfix.delta = `delta-${testParams.translator.indexVersion}`;
        translatorConfig.elastic.subfix.transaction = `action-${testParams.translator.indexVersion}`;
        translatorConfig.elastic.subfix.error = `error-${testParams.translator.indexVersion}`;
        translatorConfig.elastic.subfix.fork = `fork-${testParams.translator.indexVersion}`;
    }

    if (testParams.translator.esDumpSize)
        translatorConfig.perf.elasticDumpSize = testParams.translator.esDumpSize;

    if (testParams.translator.scrollWindow)
        translatorConfig.elastic.scrollWindow = testParams.translator.scrollWindow;

    if (testParams.translator.scrollSize)
        translatorConfig.elastic.scrollSize = testParams.translator.scrollSize;

    if (testParams.translator.totalBlocks < translatorConfig.perf.elasticDumpSize)
        translatorConfig.perf.elasticDumpSize = testParams.translator.totalBlocks;

    if (esConfig.purge) {
        // try to delete generated data in case it exists
        const reindexIndices = await esClient.cat.indices({
            index: `${testParams.translator.dstPrefix}-*`,
            format: 'json'
        });
        const deleteTasks = reindexIndices.map(
            i => esClient.indices.delete({index: i.index}));
        await Promise.all(deleteTasks);
    }

    const esDumpName = testParams.resources[0].decompressPath;
    const esDumpCompressedPath = testParams.resources[0].destinationPath;
    const esDumpPath = path.join(TEST_RESOURCES_DIR, esDumpName);
    await decompressFile(esDumpCompressedPath, esDumpPath);
    await maybeLoadElasticDump(esDumpName, esConfig);

    // launch translator and verify generated data
    const translator = new TEVMIndexer(translatorConfig);
    const translatorLaunchTime = performance.now();
    await translator.reindex(testParams.translator.dstPrefix, {eval: true, timeout: testTimeout});

    const translatorReindexTime = performance.now();

    const totalTestTimeElapsed = translatorReindexTime - testStartTime;
    const translatorReindexTimeElapsed = translatorReindexTime - translatorLaunchTime;

    console.log('Test passed!');
    console.log(`total time elapsed: ${moment.duration(totalTestTimeElapsed, 'ms').humanize()}`);
    console.log(`sync time:          ${moment.duration(translatorReindexTimeElapsed, 'ms').humanize()}`);
    console.log(`average speed:      ${(testParams.translator.totalBlocks / translatorReindexTimeElapsed).toFixed(2).toLocaleString()}`);

    await esClient.close();
}
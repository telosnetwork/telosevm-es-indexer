import {GenericContainer, StartedTestContainer, Wait} from "testcontainers";
import {TranslatorConfigSchema} from "../types/translator.js";
import {TEVMTranslator} from "../translator.js";
import {ElasticsearchContainer, StartedElasticsearchContainer} from "@testcontainers/elasticsearch";
import {Connector} from "../database/connector.js";
import fs from "node:fs";
import {expect} from "chai";
import path from "node:path";
import {fileURLToPath} from "node:url";

describe('Test Container full sync', () => {
    let chainHTTPPort = 8888;
    let chainSHIPPort = 18999;
    let leapContainer: StartedTestContainer;
    let elasticContainer: StartedElasticsearchContainer;

    let startBlock = 2;
    let stopBlock = 55;

    before(async () => {
        leapContainer = await new GenericContainer(
            "ghcr.io/telosnetwork/testcontainer-nodeos-evm:v0.1.4@sha256:a8dc857e46404d74b286f8c8d8646354ca6674daaaf9eb6f972966052c95eb4a")
            .withExposedPorts(chainHTTPPort, chainSHIPPort)
            .withName('testcontainers-leap')
            .withWaitStrategy(Wait.forLogMessage('Produced'))
            .start();

        console.log('launched chain.')

        elasticContainer = await new ElasticsearchContainer()
            .withName('testcontainers-elasticsearch')
            // .withEnvironment({
            //     'xpack.security.enabled': 'false'
            // })
            .withWaitStrategy(
                Wait.forLogMessage('Cluster health status changed from [YELLOW] to [GREEN]'))
            .start();
        console.log('launched elasticsearch.')
    });

    it ("should sync all", async () => {

        let config = TranslatorConfigSchema.parse({
            logLevel: 'debug',
            readerLogLevel: 'debug',
            chainName: 'testcontainer-chain',
            chainId: 41,
            evmBlockDelta: 1,
            startBlock,
            stopBlock: stopBlock,
            endpoint: `http://localhost:${leapContainer.getMappedPort(chainHTTPPort)}`,
            remoteEndpoint: `http://localhost:${leapContainer.getMappedPort(chainHTTPPort)}`,
            wsEndpoint: `ws://localhost:${leapContainer.getMappedPort(chainSHIPPort)}`,
            elastic: {
                node: elasticContainer.getHttpUrl()
            }
        });

        let translator = new TEVMTranslator(config);
        await translator.launch();

        await new Promise<void>(resolve => {
            translator.events.once('stop', resolve);
        });

        let conn = new Connector(config);
        let data = await conn.dumpAll(startBlock, stopBlock);

        const TESTS_DIR = path.dirname(fileURLToPath(import.meta.url));

        let validActions = fs.readFileSync(path.join(TESTS_DIR, "testcontainer-actions-v1.5.json")).toString();
        let validDeltas = fs.readFileSync(path.join(TESTS_DIR, "testcontainer-deltas-v1.5.json")).toString();

        expect(validActions, JSON.stringify(data.actions, null, 4));
        expect(validDeltas, JSON.stringify(data.blocks, null, 4));
        // fs.writeFileSync("/tmp/testcontainer-actions-v1.5.json", JSON.stringify(data.actions, null, 4));
        // fs.writeFileSync("/tmp/testcontainer-deltas-v1.5.json", JSON.stringify(data.blocks, null, 4));
    });

    after(async () => {
        leapContainer && (await leapContainer.stop());
        elasticContainer && (await elasticContainer.stop());
    });

});

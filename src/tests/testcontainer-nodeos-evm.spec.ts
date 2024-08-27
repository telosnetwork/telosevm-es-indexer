import {GenericContainer, StartedTestContainer, Wait} from "testcontainers";
import {TranslatorConfigSchema} from "../types/translator.js";
import {TEVMTranslator} from "../translator.js";
import {ElasticsearchContainer, StartedElasticsearchContainer} from "@testcontainers/elasticsearch";
import {expect} from "chai";
import {Connector} from "../database/connector.js";

describe('Test Container full sync', () => {
    let chainHTTPPort = 8888;
    let chainSHIPPort = 18999;
    let leapContainer: StartedTestContainer;
    let elasticContainer: StartedElasticsearchContainer;

    let startBlock = 2;
    let stopBlock = 53;

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
        let firstBlock = await conn.getFirstIndexedBlock();
        expect(firstBlock["@evmPrevBlockHash"], "wrong hash on genesis block")
            .to.be.eq("0000000000000000000000000000000000000000000000000000000000000000");
        expect(firstBlock["@evmBlockHash"], "wrong hash on first block")
            .to.be.eq("586be6f60f228bc1f34a488a0b8285ba52f4d8c0482dec6a1f0f46e77dbd2651");
        let lastBlock = await conn.getLastIndexedBlock();
        expect(lastBlock["@evmBlockHash"], "wrong hash on last block")
            .to.be.eq("e6d959910f531ecf246cd577e4e0d3ae8b670688a89217eb397dc35673f21e0d");
    });

    after(async () => {
        leapContainer && (await leapContainer.stop());
        elasticContainer && (await elasticContainer.stop());
    });

});

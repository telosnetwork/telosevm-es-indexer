import {GenericContainer, StartedTestContainer, Wait} from "testcontainers";
import {TranslatorConfigSchema} from "../types/translator.js";
import {TEVMTranslator} from "../translator.js";
import {ElasticsearchContainer, StartedElasticsearchContainer} from "@testcontainers/elasticsearch";
import {ChainDescriptor, ChainDescriptorSchema, JumpInfo, LeapMockClient} from "@guilledk/leap-mock";

describe('Mock Forks', () => {
    let controlPort = 6970;
    let chainHTTPPort = 8889;
    let chainSHIPPort = 18999;
    let mockerContainer: StartedTestContainer;
    let elasticContainer: StartedElasticsearchContainer;

    let mocker: LeapMockClient;

    let startBlock = 340;
    let stopBlock = 1000;

    before(async () => {
        mockerContainer = await new GenericContainer(
            "guilledk/leap-mock:0.3.0@sha256:1e3997fac996411602da01ffbc73ddd6e6d3d857f474cf607ad4d3ff19eeef25")
            // "guilledk/leap-mock:latest")
            .withExposedPorts(controlPort, chainHTTPPort, chainSHIPPort)
            .withWaitStrategy(Wait.forLogMessage("Control server running on"))
            .withName('testcontainers-leap-mock')
            .start();

        console.log('launched chain mocker.')

        const jumps: JumpInfo[] = [
            {from: 360, to: 360}, // 0 delta micro fork
            {from: 450,  to: 447}, // 3 delta micro fork
        ];
        mocker = new LeapMockClient(`http://localhost:${mockerContainer.getMappedPort(controlPort)}`);
        // mocker = new LeapMockClient(`http://localhost:${controlPort}`);
        const chainDesc: ChainDescriptor = ChainDescriptorSchema.parse({
            jumps,
            session_start_block: startBlock,
            session_stop_block: stopBlock
        });
        await mocker.setChain(chainDesc);

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

    it ("should survive forks", async () => {

        let config = TranslatorConfigSchema.parse({
            logLevel: 'debug',
            readerLogLevel: 'debug',
            chainName: 'mock-chain',
            chainId: 41,
            startBlock,
            stopBlock: stopBlock,
            endpoint: `http://localhost:${mockerContainer.getMappedPort(chainHTTPPort)}`,
            remoteEndpoint: `http://localhost:${mockerContainer.getMappedPort(chainHTTPPort)}`,
            wsEndpoint: `ws://localhost:${mockerContainer.getMappedPort(chainSHIPPort)}`,
            elastic: {
                node: elasticContainer.getHttpUrl()
            }
        });

        let translator = new TEVMTranslator(config);
        await translator.launch();

        await new Promise<void>(resolve => {
            translator.events.once('stop', resolve);
        });
    });

    after(async () => {
        mockerContainer && (await mockerContainer.stop());
    });

});

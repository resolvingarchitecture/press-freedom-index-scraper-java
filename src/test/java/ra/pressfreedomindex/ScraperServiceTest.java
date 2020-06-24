package ra.pressfreedomindex;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.*;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.messaging.MessageProducer;
import ra.common.service.ServiceStatus;
import ra.common.service.ServiceStatusListener;
import ra.util.Wait;

import java.io.IOException;
import java.util.logging.Logger;

public class ScraperServiceTest {

    private static final Logger LOG = Logger.getLogger(ScraperServiceTest.class.getName());

    private static PFIScraperService service;

    @BeforeClass
    public static void init() {
        LOG.info("Init...");
        MessageProducer producer = new MessageProducer() {
            @Override
            public boolean send(Envelope envelope) {
                LOG.info("sending envelope(id="+envelope.getId()+")");
                try {
                    Document doc = Jsoup.connect(envelope.getURL().toString()).get();
                    String html = doc.html();
                    DLC.addEntity(html, envelope);
                    envelope.getDynamicRoutingSlip().nextRoute();
                    service.handleDocument(envelope);
                } catch (IOException e) {
                    LOG.warning(e.getLocalizedMessage());
                    DLC.addErrorMessage("IOException: "+e.getLocalizedMessage(), envelope);
                }
                return true;
            }
        };
        ServiceStatusListener listener = new ServiceStatusListener() {
            @Override
            public void serviceStatusChanged(String serviceName, ServiceStatus serviceStatus) {
                LOG.info(serviceName+" status changed to "+serviceStatus.name());
            }
        };
        service = new PFIScraperService(producer, listener);
        service.start(null);
    }

    @AfterClass
    public static void tearDown() {
        LOG.info("Teardown...");
    }

    @Test
    public void verifyIndex() {
        Envelope env = Envelope.documentFactory();
        DLC.addRoute(PFIScraperService.class.getName(), PFIScraperService.OPERATION_GET_SCORE, env);
        service.handleDocument(env);
        PressFreedomIndexEntry entry = (PressFreedomIndexEntry)DLC.getEntity(env);

        Wait.aSec(2);
    }

}

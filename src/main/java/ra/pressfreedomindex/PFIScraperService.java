package ra.pressfreedomindex;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import ra.common.*;
import ra.common.content.Image;
import ra.common.route.Route;
import ra.util.Config;
import ra.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Scrapes the Press Freedom Index web site's ranking table periodically caching it
 * and serving it up to service consumers.
 */
public class PFIScraperService extends BaseService {

    private static final Logger LOG = Logger.getLogger(PFIScraperService.class.getName());

    public static final String OPERATION_GET_SCORE = "GET_SCORE";
    public static final String OPERATION_UPDATE_ENTRIES = "UPDATE_ENTRIES";
    public static final String OPERATION_GET_MAP = "UPDATE_MAP";

    private File indexDirectory;
    private URL tableURL;
    private String rawTableHTML;
    private String mapFileName;
    private String rawMapHTML;
    private Map<String,PressFreedomIndexEntry> entries = new HashMap<>();
    private Image mapImage;
    private Date entriesLastScraped;
    private Date mapLastScraped;

    public PFIScraperService() {
        super();
    }

    public PFIScraperService(MessageProducer producer, ServiceStatusListener serviceStatusListener) {
        super(producer, serviceStatusListener);
    }

    @Override
    public void handleDocument(Envelope e) {
        // Incoming from internal Service requesting local Service
        Route r = e.getRoute();
        switch (r.getOperation()) {
            case OPERATION_GET_SCORE: {
                if(entries.size()==0) {
                    DLC.addErrorMessage("NOT_READY", e);
                    break;
                }
                String countryCode = (String)DLC.getEntity(e);
                if(countryCode==null) {
                    DLC.addErrorMessage("COUNTRY_CODE_REQUIRED", e);
                    break;
                }
                PressFreedomIndexEntry entry = entries.get(countryCode);
                if(entry==null) {
                    DLC.addErrorMessage("NO_ENTRY", e);
                    break;
                }
                DLC.addEntity(entry, e);
                break;
            }
            case OPERATION_UPDATE_ENTRIES: {
                Object obj = DLC.getEntity(e);
                if(obj==null) {
                    LOG.warning("No HTML entries returned.");
                    break;
                }
                scrapeTable((String)obj);
                break;
            }
            case OPERATION_GET_MAP: {
                if(mapImage==null) {
                    mapImage = loadMap();
                }
                DLC.addEntity(mapImage, e);
                break;
            }
            default: {
                LOG.warning("Operation ("+r.getOperation()+") not supported. Sending to Dead Letter queue.");
                deadLetter(e);
            }
        }
    }

    private void scrapeTableRequest() {
        Envelope env = Envelope.documentFactory();
        DLC.addRoute(PFIScraperService.class.getName(), OPERATION_UPDATE_ENTRIES, env);
        DLC.addRoute("ra.network.NetworkService", "SEND", env);
        env.setURL(tableURL);
        producer.send(env);
    }

    private void scrapeTable(String html) {
        Document dom = Jsoup.parse(html);
        int year;
        Elements rows = dom.getElementsByTag("tr");
        PressFreedomIndexEntry entry;
        for(Element tr : rows) {
            String countryCode;
            PressFreedomIndex index;
            int position;
            double abuse;
            double situation;
            double global;
            double annualDiff;
            int annualDiffPosition;
            Elements cells = tr.getElementsByTag("td");
            LOG.info("-");
            for(Element td : cells) {
                LOG.info(td.text());
//                if(td.hasAttr("style")) {
//
//                } else if(td.hasAttr("class")) {
//
//                } else if(td.getElementsByTag("a").size() > 0) {
//                    Elements anchors = td.select("a");
//                    countryCode = Country.lookupCountryCode(anchors.get(0).text());
//                }
            }

        }
        entriesLastScraped = new Date();
    }

    private Image loadMap() {
        if(mapImage==null) {
            mapImage = new Image();
            byte[] data = FileUtil.readFileOnClasspath(this.getClass().getClassLoader(), mapFileName);
            if (data != null && data.length > 0) {
                mapImage.setBody(data, false, false);
                mapImage.setContentType("application/pdf");
                mapImage.setName(mapFileName);
            }
        }
        return mapImage;
    }

    @Override
    public boolean start(Properties p) {
        super.start(p);
        LOG.info("Starting Network Service...");
        updateStatus(ServiceStatus.STARTING);
        // Config Properties
        try {
            config = Config.loadFromClasspath("ra-press-freedom-index.config", p, false);
        } catch (Exception e) {
            LOG.warning(e.getLocalizedMessage());
            config = p;
        }

        String tableURLString = config.getProperty("ra.pressfreedomindex.url.table");
        mapFileName = config.getProperty("ra.pressfreedomindex.map");
        try {
            tableURL = new URL("https://"+tableURLString);
        } catch (MalformedURLException e) {
            LOG.severe(e.getLocalizedMessage());
            return false;
        }

        // Directories
        try {
            indexDirectory = new File(getServiceDirectory(), "db");
            if(!indexDirectory.exists() && !indexDirectory.mkdir()) {
                LOG.warning("Unable to create db directory at: "+getServiceDirectory().getAbsolutePath()+"/db");
            } else {
                config.setProperty("ra.pressfreedomindex.dir", indexDirectory.getCanonicalPath());
            }
        } catch (IOException e) {
            LOG.warning("IOException caught while building db directory: \n"+e.getLocalizedMessage());
        }

        // Load Entries and Map if present
        loadMap();
        scrapeTableRequest();

        return true;
    }

    @Override
    public boolean restart() {
        updateStatus(ServiceStatus.RESTARTING);
        gracefulShutdown();
        return true;
    }

    @Override
    public boolean shutdown() {
        super.shutdown();
        if(getServiceStatus() != ServiceStatus.RESTARTING)
            updateStatus(ServiceStatus.SHUTTING_DOWN);
        return true;
    }

    @Override
    public boolean gracefulShutdown() {
        // TODO: add wait/checks to ensure each sensor shutdowns
        return shutdown();
    }

}

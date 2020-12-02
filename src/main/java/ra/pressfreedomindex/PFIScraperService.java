package ra.pressfreedomindex;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.content.Image;
import ra.common.messaging.MessageProducer;
import ra.common.route.Route;
import ra.common.service.BaseService;
import ra.common.service.ServiceStatus;
import ra.common.service.ServiceStatusObserver;
import ra.util.Config;
import ra.util.FileUtil;
import ra.util.tasks.TaskRunner;

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
 * and serving it up to service consumers. Retrieves the index table and map image
 * via the Network Manager to ensure proper communications privacy and non-blocked status.
 */
public class PFIScraperService extends BaseService {

    private static final Logger LOG = Logger.getLogger(PFIScraperService.class.getName());

    private static final String OPERATION_PROCESS_INDEX_DOC = "PROCESS_INDEX_DOC";
    private static final String OPERATION_PROCESS_MAP_IMG = "PROCESS_INDEX_IMG";
    private static final String OPERATION_PROCESS_VERSION_CHECK = "PROCESS_VERSION_CHECK";

    public static final String OPERATION_GET_SCORE = "GET_SCORE";
    public static final String OPERATION_GET_INDEX = "GET_INDEX";
    public static final String OPERATION_GET_MAP = "GET_MAP";
    public static final String OPERATION_SAVE_INDEX = "SAVE_INDEX";
    public static final String OPERATION_SAVE_MAP = "SAVE_MAP";

    private File dbDirectory;

    private URL indexURL;
    private final String indexFileName = "pfi.json";
    private File indexFile;
    private final Map<String,PressFreedomIndexEntry> index = new HashMap<>();
    private Date indexLastScraped;

    private URL mapURL;
    private String mapName;
    private final String mapFileName = "map.json";
    private Image mapImage;
    private File mapFile;

    private Date mapLastRetrieved;

    public PFIScraperService() {
        super();
    }

    public PFIScraperService(MessageProducer producer, ServiceStatusObserver serviceStatusObserver) {
        super(producer, serviceStatusObserver);
    }

    @Override
    public void handleDocument(Envelope e) {
        // Incoming from internal Service requesting local Service
        Route r = e.getRoute();
        switch (r.getOperation()) {
            case OPERATION_PROCESS_INDEX_DOC: {

                break;
            }
            case OPERATION_PROCESS_MAP_IMG: {

                break;
            }
            case OPERATION_PROCESS_VERSION_CHECK: {

                break;
            }
            case OPERATION_GET_SCORE: {
                if(index.size()==0) {
                    DLC.addErrorMessage("NOT_READY", e);
                    break;
                }
                String countryCode = (String)DLC.getEntity(e);
                if(countryCode==null) {
                    DLC.addErrorMessage("COUNTRY_CODE_REQUIRED", e);
                    break;
                }
                PressFreedomIndexEntry entry = index.get(countryCode);
                if(entry==null) {
                    DLC.addErrorMessage("NO_ENTRY", e);
                    break;
                }
                DLC.addEntity(entry, e);
                break;
            }
            case OPERATION_GET_INDEX: {
                if(index==null || index.size()==0) {
                    scrapeIndexRequest();
                    DLC.addErrorMessage("NOT_READY", e);
                    break;
                }
                DLC.addEntity(index.values(),e);
                break;
            }
            case OPERATION_GET_MAP: {
                if(mapImage==null) {
                    loadMap();
                }
                DLC.addEntity(mapImage, e);
                break;
            }
            case OPERATION_SAVE_INDEX: {
                Object obj = DLC.getEntity(e);
                if(obj==null) {
                    LOG.warning("No HTML entries returned.");
                    break;
                }
                saveIndex((String)obj);
                break;
            }
            case OPERATION_SAVE_MAP: {
                Object obj = DLC.getEntity(e);
                if(obj==null) {
                    LOG.warning("No map returned.");
                    break;
                }
                saveMap((byte[])obj);
                break;
            }
            default: {
                LOG.warning("Operation ("+r.getOperation()+") not supported. Sending to Dead Letter queue.");
                deadLetter(e);
            }
        }
    }

    private boolean loadIndex() {
        if(index.size() == 0) {
            byte[] data = new byte[0];
            try {
                data = FileUtil.readFile(dbDirectory.getAbsolutePath());
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
            }
            if (data != null && data.length > 0) {

            } else {
                getMapRequest();
            }
        }
        return true;
    }

    private void scrapeIndexRequest() {
        Envelope env = Envelope.documentFactory();
        DLC.addRoute(PFIScraperService.class.getName(), OPERATION_SAVE_INDEX, env);
        DLC.addRoute("ra.network.NetworkService", "SEND", env);
        env.setURL(indexURL);
        producer.send(env);
    }

    private boolean saveIndex(String html) {
        if(indexFile==null) {
            indexFile = new File(dbDirectory, indexFileName);
            try {
                if(!indexFile.exists() && !indexFile.createNewFile()) {
                    LOG.warning("Unable to create index json file: "+ dbDirectory.getAbsolutePath()+"/"+indexFileName);
                    return false;
                }
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
                return false;
            }
        }
        Document dom = Jsoup.parse(html);
        int year;
        Elements rows = dom.getElementsByTag("tr");
        PressFreedomIndexEntry entry;
        StringBuilder json = new StringBuilder("{");
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
        json.append("}");
        indexLastScraped = new Date();
        return true;
    }

    private boolean loadMap() {
        if(mapImage==null) {
            if(mapFile==null) {
                mapFile = new File(dbDirectory, mapFileName);
                try {
                    if(!mapFile.exists() && !mapFile.createNewFile()) {
                        LOG.warning("Unable to create map file: "+dbDirectory+"/"+mapFileName);
                        return false;
                    }
                } catch (IOException e) {
                    LOG.warning(e.getLocalizedMessage());
                    return false;
                }
            }
            byte[] data = new byte[0];
            try {
                data = FileUtil.readFile(mapFile.getAbsolutePath());
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
            }
            if (data != null && data.length > 0) {
                mapImage = new Image();
                mapImage.setBody(data, false, false);
                mapImage.setContentType("application/pdf");
                mapImage.setName(mapName);
            } else {
                getMapRequest();
            }
        }
        return true;
    }

    private void getMapRequest() {
        Envelope env = Envelope.documentFactory();
        DLC.addRoute(PFIScraperService.class.getName(), OPERATION_SAVE_MAP, env);
        DLC.addRoute("ra.network.NetworkService", "SEND", env);
        env.setURL(mapURL);
        producer.send(env);
    }

    private boolean saveMap(byte[] pdf) {
        if(mapFile==null) {
            mapFile = new File(dbDirectory, mapFileName);
            try {
                if(!mapFile.exists() && !mapFile.createNewFile()) {
                    LOG.warning("Unable to create map file: "+dbDirectory+"/"+mapFileName);
                    return false;
                }
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
                return false;
            }
        }
        return FileUtil.writeFile(pdf, mapFile.getAbsolutePath());
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

        String indexURLString = config.getProperty("ra.pressfreedomindex.index.url");
        String mapURLString = config.getProperty("ra.pressfreedomindex.map.url");
        mapName = config.getProperty("rs.pressfreedomindex.map.name");
        try {
            indexURL = new URL("https://"+indexURLString);
            mapURL = new URL("https://"+mapURLString+mapName);
        } catch (MalformedURLException e) {
            LOG.severe(e.getLocalizedMessage());
            return false;
        }

        // Directories
        try {
            dbDirectory = new File(getServiceDirectory(), "db");
            if(!dbDirectory.exists() && !dbDirectory.mkdir()) {
                LOG.warning("Unable to create db directory at: "+getServiceDirectory().getAbsolutePath()+"/db");
            } else {
                config.setProperty("ra.pressfreedomindex.dir", dbDirectory.getCanonicalPath());
            }
        } catch (IOException e) {
            LOG.warning("IOException caught while building db directory: \n"+e.getLocalizedMessage());
        }

        // Load Map and Index
        loadMap();
        loadIndex();

        if(index==null || mapImage==null) {
            // Not loaded at this point means not persisted -> Get Index

        } else {
            // Check version to see if a new one exists to pull down

        }

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

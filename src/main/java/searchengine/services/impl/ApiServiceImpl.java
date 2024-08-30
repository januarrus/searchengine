package searchengine.services.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import searchengine.config.Connection;
import searchengine.config.Site;
import searchengine.config.SitesList;
import searchengine.model.Page;
import searchengine.model.SitePage;
import searchengine.model.Status;
import searchengine.repositories.PageRepository;
import searchengine.repositories.SiteRepository;
import searchengine.services.ApiService;
import searchengine.services.IndexingService;
import searchengine.services.LemmaService;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@RequiredArgsConstructor
@Slf4j
public class ApiServiceImpl implements ApiService {
    private final IndexingService indexingService;
    private final LemmaService lemmaService;
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;
    private final SitesList sitesToIndexing;
    private final Set<SitePage> sitePagesAllFromDB;
    private final Connection connection;
    private AtomicBoolean indexingProcessing;

    @Override
    public void startIndexing(AtomicBoolean indexingProcessing) {
        this.indexingProcessing = indexingProcessing;
        try {
            deleteSitePagesAndPagesInDB();
            addSitePagesToDB();
            indexAllSitePages();
        } catch (RuntimeException | InterruptedException ex) {
            indexingProcessing.set(false);
            log.error("Error: ", ex);
        }
    }

    @Override
    public void refreshPage(SitePage siteDomain, URL url) {
        SitePage existSitePage = siteRepository.getSitePageByUrl(siteDomain.getUrl());
        siteDomain.setId(existSitePage.getId());
        ConcurrentHashMap<String, Page> resultForkJoinPageIndexer = new ConcurrentHashMap<>();
        try {
            log.info("Запущена переиндексация страницы: " + url.toString());
            performPageRefresh(siteDomain, url, resultForkJoinPageIndexer);
        } catch (SecurityException ex) {
            handlePageRefreshError(siteDomain, ex);
        }
        markSiteAsIndexed(siteDomain);
    }

    private void performPageRefresh(SitePage siteDomain, URL url, ConcurrentHashMap<String, Page> resultForkJoinPageIndexer) {
        PageFinder finder = new PageFinder(siteRepository, pageRepository, siteDomain, url.getPath(), resultForkJoinPageIndexer, connection, lemmaService, indexingService, indexingProcessing);
        finder.refreshPage();
    }

    private void handlePageRefreshError(SitePage siteDomain, SecurityException ex) {
        SitePage sitePage = siteRepository.findById(siteDomain.getId()).orElseThrow();
        sitePage.setStatus(Status.FAILED);
        sitePage.setLastError(ex.getMessage());
        siteRepository.save(sitePage);
    }

    private void markSiteAsIndexed(SitePage siteDomain) {
        log.info("Проиндексирован сайт: " + siteDomain.getName());
        SitePage sitePage = siteRepository.findById(siteDomain.getId()).orElseThrow();
        sitePage.setStatus(Status.INDEXED);
        siteRepository.save(sitePage);
    }

    private void deleteSitePagesAndPagesInDB() {
        List<SitePage> sitesFromDB = siteRepository.findAll();
        for (SitePage sitePageDb : sitesFromDB) {
            deleteSitePageIfExistsInConfig(sitePageDb);
        }
    }

    private void deleteSitePageIfExistsInConfig(SitePage sitePageDb) {
        for (Site siteApp : sitesToIndexing.getSites()) {
            if (sitePageDb.getUrl().equals(siteApp.getUrl().toString())) {
                siteRepository.deleteById(sitePageDb.getId());
            }
        }
    }

    private void addSitePagesToDB() {
        for (Site siteApp : sitesToIndexing.getSites()) {
            saveSitePageToDB(siteApp);
        }
    }

    private void saveSitePageToDB(Site siteApp) {
        SitePage sitePageDAO = new SitePage();
        sitePageDAO.setStatus(Status.INDEXING);
        sitePageDAO.setName(siteApp.getName());
        sitePageDAO.setUrl(siteApp.getUrl().toString());
        siteRepository.save(sitePageDAO);
    }

    private void indexAllSitePages() throws InterruptedException {
        populateSitePagesFromDB();
        removeNonConfiguredSites();
        processSiteIndexing();
        indexingProcessing.set(false);
    }

    private void populateSitePagesFromDB() {
        sitePagesAllFromDB.addAll(siteRepository.findAll());
    }

    private void removeNonConfiguredSites() {
        List<String> urlsToIndex = new ArrayList<>();
        for (Site siteApp : sitesToIndexing.getSites()) {
            urlsToIndex.add(siteApp.getUrl().toString());
        }
        sitePagesAllFromDB.removeIf(sitePage -> !urlsToIndex.contains(sitePage.getUrl()));
    }

    private void processSiteIndexing() throws InterruptedException {
        List<Thread> indexingThreadList = new ArrayList<>();
        for (SitePage siteDomain : sitePagesAllFromDB) {
            indexingThreadList.add(startSiteIndexingThread(siteDomain));
        }
        waitForAllThreads(indexingThreadList);
    }

    private Thread startSiteIndexingThread(SitePage siteDomain) {
        Runnable indexSite = () -> {
            ConcurrentHashMap<String, Page> resultForkJoinPageIndexer = new ConcurrentHashMap<>();
            try {
                log.info("Запущена индексация " + siteDomain.getUrl());
                performSiteIndexing(siteDomain, resultForkJoinPageIndexer);
            } catch (SecurityException ex) {
                handleSiteIndexingError(siteDomain, ex);
            }
            finalizeSiteIndexing(siteDomain);
        };
        Thread thread = new Thread(indexSite);
        thread.start();
        return thread;
    }

    private void performSiteIndexing(SitePage siteDomain, ConcurrentHashMap<String, Page> resultForkJoinPageIndexer) {
        new ForkJoinPool().invoke(new PageFinder(siteRepository, pageRepository, siteDomain, "", resultForkJoinPageIndexer, connection, lemmaService, indexingService, indexingProcessing));
    }

    private void handleSiteIndexingError(SitePage siteDomain, SecurityException ex) {
        SitePage sitePage = siteRepository.findById(siteDomain.getId()).orElseThrow();
        sitePage.setStatus(Status.FAILED);
        sitePage.setLastError(ex.getMessage());
        siteRepository.save(sitePage);
    }

    private void finalizeSiteIndexing(SitePage siteDomain) {
        if (!indexingProcessing.get()) {
            log.warn("Indexing stopped by user, site: " + siteDomain.getUrl());
            markSiteAsFailed(siteDomain, "Indexing stopped by user");
        } else {
            markSiteAsIndexed(siteDomain);
        }
    }

    private void markSiteAsFailed(SitePage siteDomain, String errorMessage) {
        SitePage sitePage = siteRepository.findById(siteDomain.getId()).orElseThrow();
        sitePage.setStatus(Status.FAILED);
        sitePage.setLastError(errorMessage);
        siteRepository.save(sitePage);
    }

    private void waitForAllThreads(List<Thread> threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join();
        }
    }
}

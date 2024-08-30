package searchengine.services.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import searchengine.config.Connection;
import searchengine.model.Page;
import searchengine.model.SitePage;
import searchengine.repositories.PageRepository;
import searchengine.repositories.SiteRepository;
import searchengine.services.IndexingService;
import searchengine.services.LemmaService;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@RequiredArgsConstructor
public class PageFinder extends RecursiveAction {
    private final IndexingService indexingService;
    private final LemmaService lemmaService;
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;
    private final AtomicBoolean indexingProcessing;
    private final Connection connection;
    private final Set<String> urlSet = new HashSet<>();
    private final String page;
    private final SitePage siteDomain;
    private final ConcurrentHashMap<String, Page> resultForkJoinPoolIndexedPages;

    public PageFinder(SiteRepository siteRepository, PageRepository pageRepository, SitePage siteDomain, String page, ConcurrentHashMap<String, Page> resultForkJoinPoolIndexedPages, Connection connection, LemmaService lemmaService, IndexingService indexingService, AtomicBoolean indexingProcessing) {
        this.siteRepository = siteRepository;
        this.pageRepository = pageRepository;
        this.page = page;
        this.resultForkJoinPoolIndexedPages = resultForkJoinPoolIndexedPages;
        this.connection = connection;
        this.indexingProcessing = indexingProcessing;
        this.siteDomain = siteDomain;
        this.lemmaService = lemmaService;
        this.indexingService = indexingService;
    }

    @Override
    protected void compute() {
        if (isAlreadyProcessed(page)) return;

        Page indexingPage = createPage(page, siteDomain);

        try {
            Document doc = fetchDocument(indexingPage);

            processPageContent(doc, indexingPage);
            findAndProcessLinks(doc);
        } catch (Exception ex) {
            handleException(ex, indexingPage);
        }

        if (isAlreadyProcessed(page)) return;

        saveIndexingPage(indexingPage);
        processSubPages();
    }

    private boolean isAlreadyProcessed(String page) {
        return resultForkJoinPoolIndexedPages.get(page) != null || !indexingProcessing.get();
    }

    private Page createPage(String pagePath, SitePage siteDomain) {
        Page page = new Page();
        page.setPath(pagePath);
        page.setSiteId(siteDomain.getId());
        return page;
    }

    private Document fetchDocument(Page page) throws Exception {
        return Jsoup.connect(siteDomain.getUrl() + page.getPath())
                .userAgent(connection.getUserAgent())
                .referrer(connection.getReferer())
                .timeout(60000)
                .get();
    }

    private void processPageContent(Document doc, Page page) throws Exception {
        String content = doc.head() + doc.body().toString();
        if (content == null || content.isBlank()) {
            throw new Exception("Content of site id:" + page.getSiteId() + ", page:" + page.getPath() + " is null or empty");
        }

        page.setContent(content);
        page.setCode(doc.connection().response().statusCode());
    }

    private void findAndProcessLinks(Document doc) {
        Elements pages = doc.getElementsByTag("a");
        for (org.jsoup.nodes.Element element : pages) {
            String href = element.attr("href");
            if (!href.isEmpty() && href.charAt(0) == '/' && resultForkJoinPoolIndexedPages.get(href) == null) {
                urlSet.add(href);
            }
        }
    }

    private void handleException(Exception ex, Page page) {
        setPageErrorCode(page, ex);
        savePageAndLogError(page, ex);
    }

    private void setPageErrorCode(Page page, Exception ex) {
        String message = ex.toString();
        int errorCode = determineErrorCode(message);
        page.setCode(errorCode);
    }

    private int determineErrorCode(String message) {
        if (message.contains("UnsupportedMimeTypeException")) return 415;
        if (message.contains("Status=401")) return 401;
        if (message.contains("UnknownHostException")) return 401;
        if (message.contains("Status=403")) return 403;
        if (message.contains("Status=404")) return 404;
        if (message.contains("Status=500")) return 500;
        if (message.contains("ConnectException: Connection refused")) return 500;
        if (message.contains("SSLHandshakeException")) return 525;
        if (message.contains("Status=503")) return 503;
        return -1;
    }

    private void savePageAndLogError(Page page, Exception ex) {
        SitePage sitePage = siteRepository.findById(siteDomain.getId()).orElseThrow();
        sitePage.setStatusTime(Timestamp.valueOf(LocalDateTime.now()));
        siteRepository.save(sitePage);
        pageRepository.save(page);
        log.debug("ERROR INDEXATION, siteId:{}, path:{}, code:{}, error:{}", page.getSiteId(), page.getPath(), page.getCode(), ex.getMessage());
    }

    private void saveIndexingPage(Page page) {
        resultForkJoinPoolIndexedPages.putIfAbsent(page.getPath(), page);
        SitePage sitePage = siteRepository.findById(siteDomain.getId()).orElseThrow();
        sitePage.setStatusTime(Timestamp.valueOf(LocalDateTime.now()));
        siteRepository.save(sitePage);
        pageRepository.save(page);
        indexingService.indexHtml(page.getContent(), page);
    }

    private void processSubPages() {
        List<PageFinder> tasks = new ArrayList<>();
        for (String url : urlSet) {
            if (resultForkJoinPoolIndexedPages.get(url) == null && indexingProcessing.get()) {
                PageFinder task = new PageFinder(siteRepository, pageRepository, siteDomain, url,
                                                 resultForkJoinPoolIndexedPages, connection, lemmaService,
                                                 indexingService, indexingProcessing);
                task.fork();
                tasks.add(task);
            }
        }
        joinTasks(tasks);
    }

    private void joinTasks(List<PageFinder> tasks) {
        for (PageFinder task : tasks) {
            if (!indexingProcessing.get()) return;
            task.join();
        }
    }

    public void refreshPage() {
        Page page = createPage(this.page, siteDomain);

        try {
            Document doc = fetchDocument(page);
            processPageContent(doc, page);
        } catch (Exception ex) {
            handleException(ex, page);
            return;
        }

        SitePage sitePage = siteRepository.findById(siteDomain.getId()).orElseThrow();
        sitePage.setStatusTime(Timestamp.valueOf(LocalDateTime.now()));
        siteRepository.save(sitePage);

        Page existingPage = pageRepository.findPageBySiteIdAndPath(page.getPath(), sitePage.getId());
        saveOrUpdatePage(existingPage, page);
    }

    private void saveOrUpdatePage(Page existingPage, Page newPage) {
        if (existingPage != null) {
            existingPage.setCode(newPage.getCode());
            existingPage.setContent(newPage.getContent());
            pageRepository.save(existingPage);
            indexingService.refreshIndex(newPage.getContent(), existingPage);
        } else {
            pageRepository.save(newPage);
            indexingService.refreshIndex(newPage.getContent(), newPage);
        }
    }
}

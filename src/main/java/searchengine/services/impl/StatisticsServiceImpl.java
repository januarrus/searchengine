package searchengine.services.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import searchengine.config.Site;
import searchengine.config.SitesList;
import searchengine.dto.statistics.DetailedStatisticsItem;
import searchengine.dto.statistics.StatisticsData;
import searchengine.dto.statistics.StatisticsResponse;
import searchengine.dto.statistics.TotalStatistics;
import searchengine.model.SitePage;
import searchengine.repositories.LemmaRepository;
import searchengine.repositories.PageRepository;
import searchengine.repositories.SiteRepository;
import searchengine.services.StatisticsService;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class StatisticsServiceImpl implements StatisticsService {
    private final SitesList sites;
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;

    @Override
    public StatisticsResponse getStatistics() throws MalformedURLException {
        List<SitePage> sitePages = siteRepository.findAll();
        TotalStatistics total = new TotalStatistics();
        List<DetailedStatisticsItem> detailed = new ArrayList<>();

        if (sitePages.isEmpty()) {
            return createStartStatistics();
        }

        total.setSites(sites.getSites().size());
        total.setIndexing(true);

        for (SitePage sitePage : sitePages) {
            DetailedStatisticsItem item = createDetailedStatisticsItem(sitePage);
            total.setPages(total.getPages() + item.getPages());
            total.setLemmas(total.getLemmas() + item.getLemmas());
            detailed.add(item);
        }

        StatisticsResponse response = new StatisticsResponse();
        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);
        response.setStatistics(data);
        response.setResult(true);

        return response;
    }

    private DetailedStatisticsItem createDetailedStatisticsItem(SitePage sitePage) throws MalformedURLException {
        Site site = new Site();
        site.setName(sitePage.getName());
        site.setUrl(new URL(sitePage.getUrl()));

        DetailedStatisticsItem item = new DetailedStatisticsItem();
        item.setName(site.getName());
        item.setUrl(site.getUrl().toString());
        item.setPages(pageRepository.findCountRecordBySiteId(sitePage.getId()));
        item.setLemmas(lemmaRepository.findCountRecordBySiteId(sitePage.getId()));
        item.setStatus(String.valueOf(sitePage.getStatus()));
        item.setError(sitePage.getLastError());
        item.setStatusTime(sitePage.getStatusTime().getTime());

        return item;
    }

    private StatisticsResponse createStartStatistics() {
        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setIndexing(false);

        List<DetailedStatisticsItem> detailed = sites.getSites().stream()
                .map(this::createInitialStatisticsItem)
                .toList();

        StatisticsResponse response = new StatisticsResponse();
        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);
        response.setStatistics(data);
        response.setResult(true);

        return response;
    }

    private DetailedStatisticsItem createInitialStatisticsItem(Site site) {
        DetailedStatisticsItem item = new DetailedStatisticsItem();
        item.setName(site.getName());
        item.setUrl(site.getUrl().toString());
        item.setPages(0);
        item.setLemmas(0);
        item.setStatus("WAIT");
        item.setError(null);
        item.setStatusTime(Instant.now().toEpochMilli());

        return item;
    }
}

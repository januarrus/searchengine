package searchengine.services.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import searchengine.dto.RankDto;
import searchengine.dto.responses.NotOkResponse;
import searchengine.dto.responses.SearchDataResponse;
import searchengine.dto.responses.SearchResponse;
import searchengine.model.IndexSearch;
import searchengine.model.Lemma;
import searchengine.model.SitePage;
import searchengine.model.Status;
import searchengine.repositories.IndexSearchRepository;
import searchengine.repositories.LemmaRepository;
import searchengine.repositories.PageRepository;
import searchengine.repositories.SiteRepository;
import searchengine.services.LemmaService;
import searchengine.services.SearchService;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class SearchServiceImpl implements SearchService {
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexSearchRepository indexRepository;
    private final LemmaService lemmaService;
    private final Status indexSuccessStatus = Status.INDEXED;
    private final double frequencyLimitProportion = 100.0;

    @Override
    public ResponseEntity<Object> search(String query, String site, Integer offset, Integer limit) throws IOException {
        if (checkIndexStatusNotIndexed(site)) {
            return ResponseEntity.badRequest().body(new NotOkResponse("Индексация сайта для поиска не закончена"));
        }

        SitePage siteTarget = siteRepository.getSitePageByUrl(site);
        Integer countPages = siteTarget != null ? pageRepository.getCountPages(siteTarget.getId()) : pageRepository.getCountPages(null);

        List<Lemma> sortedLemmasToSearch = getFilteredLemmasForSearch(query, siteTarget)
                .stream()
                .sorted(Comparator.comparingInt(Lemma::getFrequency))
                .collect(Collectors.toList());

        if (sortedLemmasToSearch.isEmpty()) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        Map<Integer, IndexSearch> indexesByLemmas = getIndexesByLemmas(sortedLemmasToSearch);
        if (indexesByLemmas.isEmpty()) {
            return ResponseEntity.ok(new SearchResponse(true, 0, Collections.emptyList()));
        }

        List<SearchDataResponse> searchDataResponses = buildSearchDataResponses(calculatePageRelevance(indexesByLemmas), query);
        List<SearchDataResponse> paginatedResults = paginateResults(searchDataResponses, offset, limit);

        return ResponseEntity.ok(paginatedResults);
    }

    private List<Lemma> getFilteredLemmasForSearch(String query, SitePage siteTarget) {
        try {
            return lemmaService.getLemmasFromText(query).keySet().stream()
                    .map(it -> lemmaRepository.findLemmasByLemmaAndSiteId(it, siteTarget != null ? siteTarget.getId() : null))
                    .flatMap(Collection::stream)
                    .filter(e -> {
                        Integer lemmaFrequency = lemmaRepository.findCountPageByLemma(e.getLemma(), e.getSiteId());
                        return lemmaFrequency == null || (double) lemmaFrequency / (siteTarget != null ? pageRepository.getCountPages(siteTarget.getId()) : pageRepository.getCountPages(null)) <= frequencyLimitProportion;
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Integer, IndexSearch> getIndexesByLemmas(List<Lemma> sortedLemmasToSearch) {
        Map<Integer, IndexSearch> indexesByLemmas = indexRepository.findIndexesByLemma(sortedLemmasToSearch.get(0).getId())
                .stream()
                .collect(Collectors.toMap(IndexSearch::getPageId, index -> index));

        for (int i = 1; i < sortedLemmasToSearch.size(); i++) {
            List<Integer> pagesToSave = indexRepository.findIndexesByLemma(sortedLemmasToSearch.get(i).getId()).stream()
                    .map(IndexSearch::getPageId)
                    .filter(indexesByLemmas::containsKey)
                    .collect(Collectors.toList());

            indexesByLemmas.keySet().retainAll(pagesToSave);
        }

        return indexesByLemmas;
    }

    private Set<RankDto> calculatePageRelevance(Map<Integer, IndexSearch> indexesByLemmas) {
        Map<Integer, RankDto> pageRelevanceMap = new HashMap<>();
        for (IndexSearch index : indexesByLemmas.values()) {
            RankDto rankPage = pageRelevanceMap.computeIfAbsent(index.getPageId(), id -> {
                RankDto rank = new RankDto();
                rank.setPage(index.getPage());
                return rank;
            });
            rankPage.setPageId(index.getPageId());
            rankPage.setAbsRelevance(rankPage.getAbsRelevance() + index.getLemmaCount());
            rankPage.setMaxLemmaRank(Math.max(rankPage.getMaxLemmaRank(), index.getLemmaCount()));
        }

        pageRelevanceMap.values().forEach(rank -> rank.setRelativeRelevance(rank.getAbsRelevance() / rank.getMaxLemmaRank()));
        return new HashSet<>(pageRelevanceMap.values());
    }

    private List<SearchDataResponse> buildSearchDataResponses(Set<RankDto> pagesRelevance, String query) {
        List<String> simpleLemmasFromSearch = null;
        try {
            simpleLemmasFromSearch = lemmaService.getLemmasFromText(query).keySet().stream().collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> finalSimpleLemmasFromSearch = simpleLemmasFromSearch;
        return pagesRelevance.stream()
                .flatMap(rank -> {
                    Document doc = Jsoup.parse(rank.getPage().getContent());
                    return doc.body().getElementsMatchingOwnText("[\\p{IsCyrillic}]").stream()
                            .map(Element::text)
                            .map(sentence -> {
                                StringBuilder textFromElement = new StringBuilder(sentence);
                                List<String> words = Arrays.asList(sentence.split("[\\s:punct]"));
                                long searchWordsCount = words.stream()
                                        .map(word -> lemmaService.getLemmaByWord(word.replaceAll("\\p{Punct}", "")))
                                        .filter(finalSimpleLemmasFromSearch::contains)
                                        .peek(word -> markWord(textFromElement, word, 0))
                                        .count();

                                return searchWordsCount > 0 ? new SearchDataResponse(
                                        siteRepository.findById(pageRepository.findById(rank.getPageId()).get().getSiteId()).get().getUrl(),
                                        siteRepository.findById(pageRepository.findById(rank.getPageId()).get().getSiteId()).get().getName(),
                                        rank.getPage().getPath(),
                                        doc.title(),
                                        textFromElement.toString(),
                                        rank.getRelativeRelevance(),
                                        (int) searchWordsCount
                                ) : null;
                            })
                            .filter(Objects::nonNull);
                })
                .collect(Collectors.toList());
    }

    private List<SearchDataResponse> paginateResults(List<SearchDataResponse> searchDataResponses, Integer offset, Integer limit) {
        return searchDataResponses.stream()
                .sorted(Comparator.comparingDouble(SearchDataResponse::getRelevance).reversed())
                .skip((long) offset * limit)
                .limit(limit)
                .sorted(Comparator.comparingInt(SearchDataResponse::getWordsFound).reversed())
                .collect(Collectors.toList());
    }

    private Boolean checkIndexStatusNotIndexed(String site) {
        return site == null || site.isBlank()
                ? siteRepository.findAll().stream().anyMatch(s -> !s.getStatus().equals(indexSuccessStatus))
                : !siteRepository.getSitePageByUrl(site).getStatus().equals(indexSuccessStatus);
    }

    private void markWord(StringBuilder textFromElement, String word, int startPosition) {
        int start = textFromElement.indexOf(word, startPosition);
        if (start == -1) return;

        if (textFromElement.indexOf("<b>", start - 3) == (start - 3)) {
            markWord(textFromElement, word, start + word.length());
            return;
        }

        int end = start + word.length();
        textFromElement.insert(start, "<b>");
        textFromElement.insert(end + 3, "</b>");
    }
}

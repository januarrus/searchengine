package searchengine.services.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import searchengine.model.IndexSearch;
import searchengine.model.Lemma;
import searchengine.model.Page;
import searchengine.repositories.IndexSearchRepository;
import searchengine.repositories.LemmaRepository;
import searchengine.services.LemmaService;
import searchengine.services.IndexingService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class PageIndexerServiceImpl implements IndexingService {
    private final LemmaService lemmaService;
    private final LemmaRepository lemmaRepository;
    private final IndexSearchRepository indexSearchRepository;

    @Override
    public void indexHtml(String html, Page indexingPage) {
        long start = System.currentTimeMillis();
        try {
            Map<String, Integer> lemmas = lemmaService.getLemmasFromText(html);
            lemmas.entrySet().parallelStream().forEach(entry -> saveLemma(entry.getKey(), entry.getValue(), indexingPage));
            log.debug("Индексация страницы заняла " + (System.currentTimeMillis() - start) + " мс, количество лемм: " + lemmas.size());
        } catch (IOException e) {
            log.error("Ошибка при индексации HTML: ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void refreshIndex(String html, Page refreshPage) {
        long start = System.currentTimeMillis();
        try {
            Map<String, Integer> lemmas = lemmaService.getLemmasFromText(html);
            refreshLemma(refreshPage);
            indexSearchRepository.deleteAllByPageId(refreshPage.getId());
            lemmas.entrySet().parallelStream().forEach(entry -> saveLemma(entry.getKey(), entry.getValue(), refreshPage));
            log.debug("Обновление индекса страницы заняло " + (System.currentTimeMillis() - start) + " мс, количество лемм: " + lemmas.size());
        } catch (IOException e) {
            log.error("Ошибка при обновлении индекса HTML: ", e);
            throw new RuntimeException(e);
        }
    }

    @Transactional
    private void refreshLemma(Page refreshPage) {
        List<IndexSearch> indexes = indexSearchRepository.findAllByPageId(refreshPage.getId());
        indexes.forEach(index -> {
            Optional<Lemma> lemmaToRefresh = lemmaRepository.findById(index.getLemmaId());
            lemmaToRefresh.ifPresent(lemma -> {
                lemma.setFrequency(lemma.getFrequency() - index.getLemmaCount());
                lemmaRepository.saveAndFlush(lemma);
            });
        });
    }

    @Transactional
    private void saveLemma(String lemmaText, Integer frequency, Page page) {
        Lemma existingLemma = lemmaRepository.lemmaExist(lemmaText, page.getSiteId());
        if (existingLemma != null) {
            existingLemma.setFrequency(existingLemma.getFrequency() + frequency);
            lemmaRepository.saveAndFlush(existingLemma);
            createIndex(page, existingLemma, frequency);
        } else {
            try {
                Lemma newLemma = new Lemma();
                newLemma.setSiteId(page.getSiteId());
                newLemma.setLemma(lemmaText);
                newLemma.setFrequency(frequency);
                newLemma.setSitePage(page.getSitePage());
                lemmaRepository.saveAndFlush(newLemma);
                createIndex(page, newLemma, frequency);
            } catch (DataIntegrityViolationException ex) {
                log.debug("Ошибка при сохранении леммы. Попробую снова.", ex);
                saveLemma(lemmaText, frequency, page);
            }
        }
    }

    private void createIndex(Page page, Lemma lemma, Integer rank) {
        IndexSearch existingIndex = indexSearchRepository.indexSearchExist(page.getId(), lemma.getId());
        if (existingIndex != null) {
            existingIndex.setLemmaCount(existingIndex.getLemmaCount() + rank);
            indexSearchRepository.save(existingIndex);
        } else {
            IndexSearch index = new IndexSearch();
            index.setPageId(page.getId());
            index.setLemmaId(lemma.getId());
            index.setLemmaCount(rank);
            index.setLemma(lemma);
            index.setPage(page);
            indexSearchRepository.save(index);
        }
    }
}

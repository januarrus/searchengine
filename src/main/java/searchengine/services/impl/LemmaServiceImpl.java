package searchengine.services.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.WrongCharaterException;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.jsoup.Jsoup;
import org.springframework.stereotype.Service;
import searchengine.services.LemmaService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class LemmaServiceImpl implements LemmaService {
    private final LuceneMorphology luceneMorphology;

    public LemmaServiceImpl() {
        this.luceneMorphology = initializeMorphology();
    }

    private LuceneMorphology initializeMorphology() {
        try {
            return new RussianLuceneMorphology();
        } catch (IOException e) {
            log.error("Failed to initialize RussianLuceneMorphology", e);
            throw new RuntimeException("Failed to initialize RussianLuceneMorphology", e);
        }
    }

    @Override
    public Map<String, Integer> getLemmasFromText(String html) {
        Map<String, Integer> lemmasInText = new HashMap<>();
        String text = extractTextFromHtml(html);
        List<String> words = extractWordsFromText(text);
        words.forEach(word -> determineLemma(word, lemmasInText));
        return lemmasInText;
    }

    private String extractTextFromHtml(String html) {
        return Jsoup.parse(html).text();
    }

    private List<String> extractWordsFromText(String text) {
        return new ArrayList<>(List.of(text.toLowerCase().split("[^a-zа-я]+")));
    }

    @Override
    public String getLemmaByWord(String word) {
        String preparedWord = word.toLowerCase();
        if (checkMatchWord(preparedWord)) return "";

        return extractLemma(preparedWord);
    }

    private String extractLemma(String word) {
        try {
            List<String> normalWordForms = luceneMorphology.getNormalForms(word);
            String wordInfo = getWordInfo(word);
            if (checkWordInfo(wordInfo)) return "";
            return normalWordForms.get(0);
        } catch (WrongCharaterException ex) {
            log.debug("Error processing word '{}': {}", word, ex.getMessage());
        }
        return "";
    }

    private String getWordInfo(String word) {
        return luceneMorphology.getMorphInfo(word).toString();
    }

    private void determineLemma(String word, Map<String, Integer> lemmasInText) {
        try {
            if (checkMatchWord(word)) return;

            String normalWord = getNormalFormOfWord(word);
            incrementLemmaCount(normalWord, lemmasInText);
        } catch (RuntimeException ex) {
            log.debug("Error processing word '{}': {}", word, ex.getMessage());
        }
    }

    private String getNormalFormOfWord(String word) {
        List<String> normalWordForms = luceneMorphology.getNormalForms(word);
        String wordInfo = getWordInfo(word);
        if (checkWordInfo(wordInfo)) return "";
        return normalWordForms.get(0);
    }

    private void incrementLemmaCount(String lemma, Map<String, Integer> lemmasInText) {
        lemmasInText.put(lemma, lemmasInText.getOrDefault(lemma, 0) + 1);
    }

    private boolean checkMatchWord(String word) {
        return word.isEmpty() || isEnglishWord(word) || isNumeric(word);
    }

    private boolean isEnglishWord(String word) {
        return String.valueOf(word.charAt(0)).matches("[a-z]");
    }

    private boolean isNumeric(String word) {
        return String.valueOf(word.charAt(0)).matches("[0-9]");
    }

    private boolean checkWordInfo(String wordInfo) {
        return wordInfo.contains("ПРЕДЛ") || wordInfo.contains("СОЮЗ") || wordInfo.contains("МЕЖД");
    }
}

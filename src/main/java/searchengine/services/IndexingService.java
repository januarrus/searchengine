package searchengine.services;

import searchengine.model.Page;

public interface IndexingService {
    void indexHtml(String html, Page inexingPage);
    void refreshIndex(String html, Page refreshPage);
}

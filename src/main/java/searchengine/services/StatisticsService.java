package searchengine.services;

import org.springframework.stereotype.Service;
import searchengine.dto.statistics.StatisticsResponse;

import java.net.MalformedURLException;

@Service
public interface StatisticsService {
    StatisticsResponse getStatistics() throws MalformedURLException;
}

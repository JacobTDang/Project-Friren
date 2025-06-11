from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline
from news_sentiment import NewsSentimentCollector, NewsArticle
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import torch
import re
import warnings

# Suppress the deprecation warning for return_all_scores
warnings.filterwarnings("ignore", message=".*return_all_scores.*deprecated.*")

@dataclass
class SentimentResult:
    """
    Store the sentiment data
    """
    text: str
    label: str
    score: float
    raw_scores: Dict[str, float]
    timestamp: datetime
    symbols_mentioned: Optional[List[str]] = None

class EnhancedFinBERT:
    def __init__(self,
                 model_name: str = "yiyanghkust/finbert-tone",
                 device: str = "auto",
                 max_length: int = 512,
                 batch_size: int = 16):
        self.model_name = model_name
        self.max_length = max_length
        self.batch_size = batch_size

        if device == "auto":
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            self.device = device

        print(f"Initializing FinBERT on {self.device.upper()}...")
        self.load_model()
        self.collector = NewsSentimentCollector()
        self.sentiment_cache = {}
        print("FinBERT ready for analysis!")

    def load_model(self):
        try:
            print("Loading FinBERT model...")
            tokenizer = BertTokenizer.from_pretrained(self.model_name)
            model = BertForSequenceClassification.from_pretrained(self.model_name)
            self.tokenizer = tokenizer
            self.model = model

            # Move to device and set eval mode
            self.model.to(self.device)
            self.model.eval()

            # Use top_k instead of deprecated return_all_scores
            self.nlp = pipeline(
                "sentiment-analysis",
                model=self.model,
                tokenizer=self.tokenizer,
                device=0 if self.device == "cuda" else -1,
                top_k=None,  # Returns all scores (replaces return_all_scores=True)
                truncation=True,
                max_length=self.max_length
            )

            print("FinBERT loaded successfully")
        except Exception as e:
            print(f'Error loading FinBERT: {e}')
            raise

    def preprocess_text(self, text: str):
        if not text:
            return ""
        # Remove URLs
        text = re.sub(r'http[s]?://\S+', '', text)
        # Clean whitespace
        text = ' '.join(text.split())
        # Truncate if too long
        if len(text) > 500:
            return text[:500] + "..."
        return text.strip()

    def analyze_single_text(self, text: str, symbols: List[str] = None) -> SentimentResult:
        cache_key = hash(text)
        if cache_key in self.sentiment_cache:
            cached_result = self.sentiment_cache[cache_key]
            # Update metadata for cached result
            cached_result.symbols_mentioned = symbols or []
            cached_result.timestamp = datetime.now()
            return cached_result

        cleaned_text = self.preprocess_text(text)

        if not cleaned_text:
            return SentimentResult(
                text=text,
                label="neutral",
                score=0.0,
                raw_scores={"positive": 0.33, "negative": 0.33, "neutral": 0.34},
                timestamp=datetime.now(),
                symbols_mentioned=symbols or []
            )

        try:
            # Get all sentiment scores
            results = self.nlp(cleaned_text)[0]  # First result from batch

            # Convert to our format (handle both old and new pipeline formats)
            if isinstance(results, list):
                raw_scores = {result['label'].lower(): result['score'] for result in results}
            else:
                # Fallback for different pipeline formats
                raw_scores = {"positive": 0.33, "negative": 0.33, "neutral": 0.34}

            best_label = max(raw_scores, key=raw_scores.get)
            best_score = raw_scores[best_label]

            sentiment_result = SentimentResult(
                text=text,
                label=best_label,
                score=best_score,
                raw_scores=raw_scores,
                timestamp=datetime.now(),
                symbols_mentioned=symbols or []
            )

            # Cache the result
            self.sentiment_cache[cache_key] = sentiment_result
            return sentiment_result

        except Exception as e:
            print(f'Error analyzing text: {e}')
            return SentimentResult(
                text=text,
                label="neutral",
                score=0.0,
                raw_scores={"positive": 0.33, "negative": 0.33, "neutral": 0.34},
                timestamp=datetime.now(),
                symbols_mentioned=symbols or []
            )

    def analyze_batch(self, texts: List[str], symbols_list: List[List[str]] = None):
        if not texts:
            return []
        if symbols_list is None:
            symbols_list = [[] for _ in texts]

        results = []
        for i in range(0, len(texts), self.batch_size):
            batch_texts = texts[i:i + self.batch_size]
            batch_symbols = symbols_list[i:i + self.batch_size]
            for text, symbols in zip(batch_texts, batch_symbols):
                result = self.analyze_single_text(text, symbols)
                results.append(result)

        print(f"Analyzed {len(results)} texts in batch")
        return results

    def analyze_articles(self, articles: List[NewsArticle]):
        if not articles:
            print("No articles to analyze")
            return articles

        print(f"Analyzing sentiment for {len(articles)} articles...")

        # Extract valid articles with titles
        valid_articles = [a for a in articles if a.title and a.title.strip()]
        if not valid_articles:
            print("No valid articles with titles found")
            return articles

        texts = [article.title for article in valid_articles]
        symbol_list = [article.symbols_mentioned for article in valid_articles]

        sentiment_results = self.analyze_batch(texts, symbol_list)

        # Update articles with sentiment data
        for article, sentiment in zip(valid_articles, sentiment_results):
            article.sentiment_score = sentiment.score
            article.sentiment_label = sentiment.label
            article.sentiment_confidence = sentiment.score
            if hasattr(article, 'raw_sentiment_scores'):
                article.raw_sentiment_scores = sentiment.raw_scores

        print(f"Updated {len(valid_articles)} articles with sentiment data")
        return articles

    def _log_sentiment_distribution(self, articles: List[NewsArticle]):
        sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
        analyzed_count = 0

        for article in articles:
            if hasattr(article, 'sentiment_label') and article.sentiment_label:
                sentiment_counts[article.sentiment_label] += 1
                analyzed_count += 1

        if analyzed_count > 0:
            print(f"Sentiment Distribution (n={analyzed_count}):")
            for label, count in sentiment_counts.items():
                pct = (count / analyzed_count) * 100
                print(f"  {label.capitalize()}: {count} ({pct:.1f}%)")

    def collect_and_analyze_news(self, hours_back: int = 6):
        try:
            print(f"Collecting news from the last {hours_back} hours...")
            articles = self.collector.collect_all_news(hours_back=hours_back)

            if not articles:
                print("No articles collected")
                return []

            print(f"Collected {len(articles)} articles")
            analyzed_articles = self.analyze_articles(articles)
            self._log_sentiment_distribution(analyzed_articles)
            return analyzed_articles

        except Exception as e:
            print(f"Error in collect_and_analyze_news: {e}")
            return []

    def get_market_sentiment_signal(self,
                                    articles: List[NewsArticle],
                                    symbol: str = None,
                                    min_articles: int = 5):
        relevant_articles = articles

        if symbol:
            relevant_articles = [
                article for article in articles
                if symbol.upper() in [s.upper() for s in article.symbols_mentioned]
            ]

        analyzed_articles = [
            article for article in relevant_articles
            if hasattr(article, 'sentiment_label') and article.sentiment_label
        ]

        if len(analyzed_articles) < min_articles:
            return {
                "signal": "NEUTRAL",
                "confidence": 0.0,
                "article_count": len(analyzed_articles),
                "sentiment_scores": {"positive": 0.0, "negative": 0.0, "neutral": 0.0},
                "symbol": symbol
            }

        # Calculate weighted sentiment scores
        positive_score = sum(article.sentiment_score for article in analyzed_articles if article.sentiment_label == "positive")
        negative_score = sum(article.sentiment_score for article in analyzed_articles if article.sentiment_label == "negative")
        neutral_score = sum(article.sentiment_score for article in analyzed_articles if article.sentiment_label == "neutral")

        total_score = positive_score + negative_score + neutral_score
        if total_score == 0:
            return {
                "signal": "NEUTRAL",
                "confidence": 0.0,
                "article_count": len(analyzed_articles),
                "sentiment_scores": {"positive": 0.0, "negative": 0.0, "neutral": 0.0},
                "symbol": symbol
            }

        # Normalize scores
        norm_positive = positive_score / total_score
        norm_negative = negative_score / total_score
        norm_neutral = neutral_score / total_score

        # Generate signal
        if norm_positive > 0.5:
            signal = "BULLISH"
            confidence = norm_positive
        elif norm_negative > 0.5:
            signal = "BEARISH"
            confidence = norm_negative
        else:
            signal = "NEUTRAL"
            confidence = max(norm_positive, norm_negative, norm_neutral)

        return {
            "signal": signal,
            "confidence": confidence,
            "article_count": len(analyzed_articles),
            "sentiment_scores": {
                "positive": norm_positive,
                "negative": norm_negative,
                "neutral": norm_neutral
            },
            "symbol": symbol,
            "timestamp": datetime.now()
        }

    def clear_cache(self):
        self.sentiment_cache.clear()
        print("Sentiment cache cleared")

def test_enhanced_finbert():
    print("Testing Enhanced FinBERT Analysis...")
    finbert = EnhancedFinBERT()

    # Test single text analysis
    test_text = "Apple reports record quarterly earnings, beating analyst expectations"
    result = finbert.analyze_single_text(test_text, symbols=["AAPL"])

    print(f"\nSingle Text Analysis:")
    print(f"Text: {test_text}")
    print(f"Label: {result.label}")
    print(f"Score: {result.score:.3f}")
    print(f"Raw scores: {result.raw_scores}")

    # Test news collection and analysis
    print(f"\nCollecting and analyzing recent news...")
    articles = finbert.collect_and_analyze_news(hours_back=6)

    if articles:
        print(f"Analyzed {len(articles)} articles")
        analyzed_articles = [a for a in articles if hasattr(a, 'sentiment_label')]
        for i, article in enumerate(analyzed_articles[:3]):
            print(f"\n--- Article {i+1} ---")
            print(f"Title: {article.title}")
            print(f"Source: {article.source}")
            print(f"Sentiment: {article.sentiment_label} ({article.sentiment_score:.3f})")
            print(f"Symbols: {article.symbols_mentioned}")

        # Test market sentiment signal
        market_signal = finbert.get_market_sentiment_signal(articles)
        print(f"\nOverall Market Sentiment Signal:")
        print(f"Signal: {market_signal['signal']}")
        print(f"Confidence: {market_signal['confidence']:.3f}")
        print(f"Based on {market_signal['article_count']} articles")

        # Test symbol-specific signal
        if any(article.symbols_mentioned for article in articles):
            from collections import Counter
            all_symbols = []
            for article in articles:
                all_symbols.extend(article.symbols_mentioned)
            if all_symbols:
                common_symbol = Counter(all_symbols).most_common(1)[0][0]
                symbol_signal = finbert.get_market_sentiment_signal(articles, symbol=common_symbol)
                print(f"\nSentiment Signal for {common_symbol}:")
                print(f"Signal: {symbol_signal['signal']}")
                print(f"Confidence: {symbol_signal['confidence']:.3f}")
                print(f"Based on {symbol_signal['article_count']} articles")
    else:
        print("No articles collected for analysis")

if __name__ == "__main__":
    test_enhanced_finbert()

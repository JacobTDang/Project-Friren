"""
trading_engine/sentiment/finBERT_analysis.py

FinBERT Analysis Utility Tool - Pure Business Logic

Handles FinBERT model loading, text preprocessing, and sentiment analysis.
No process logic - just sentiment analysis utilities.
"""

import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import numpy as np
from datetime import datetime

# Import configuration manager to eliminate ALL hardcoded values
from Friren_V1.infrastructure.configuration_manager import get_config

# Import API resilience system
try:
    from Friren_V1.infrastructure.api_resilience import (
        APIServiceType, get_resilience_manager
    )
    HAS_RESILIENCE = True
except ImportError:
    HAS_RESILIENCE = False


@dataclass
class SentimentResult:
    """Individual article sentiment analysis result"""
    article_id: str
    text: str
    sentiment_score: float  # -1.0 to +1.0 (negative to positive)
    confidence: float      # 0.0 to 1.0
    classification: str    # POSITIVE, NEGATIVE, NEUTRAL
    raw_scores: Dict[str, float]  # Raw model outputs
    processing_time: float
    model_version: str
    is_reliable: bool      # True if confidence > threshold


@dataclass
class BatchSentimentResult:
    """Batch sentiment analysis result"""
    results: List[SentimentResult]
    batch_processing_time: float
    success_count: int
    error_count: int
    average_confidence: float
    sentiment_distribution: Dict[str, int]  # Count by classification


class EnhancedFinBERT:
    """
    Enhanced FinBERT Sentiment Analysis Tool

    Pure utility class for financial sentiment analysis using FinBERT.
    Optimized for trading system integration with proper error handling,
    batching, and performance monitoring.

    Features:
    - FinBERT model loading with fallback options
    - Batch processing for efficiency
    - Text preprocessing and cleaning
    - Confidence scoring and classification
    - Performance monitoring and caching
    - Memory-aware processing for t3.micro
    """

    def __init__(self, model_name: Optional[str] = None,
                 max_length: Optional[int] = None,
                 batch_size: Optional[int] = None,
                 device: str = "cpu"):
        
        # PRODUCTION: Get FinBERT configuration - NO HARDCODED VALUES
        try:
            from Friren_V1.infrastructure.configuration_manager import get_finbert_config
            finbert_config = get_finbert_config()
        except ImportError:
            raise ImportError("CRITICAL: Configuration manager required for FinBERT. No hardcoded values allowed.")
        
        # Use configuration values or provided parameters
        self.model_name = model_name if model_name is not None else finbert_config['model_name']
        self.max_length = max_length if max_length is not None else finbert_config['max_length']
        self.batch_size = batch_size if batch_size is not None else finbert_config['batch_size']
        self.device = device
        
        # Validate critical configuration
        if not self.model_name:
            raise ValueError("PRODUCTION: FINBERT_MODEL_NAME must be configured")
        if not self.max_length or self.max_length <= 0:
            raise ValueError("PRODUCTION: FINBERT_MAX_LENGTH must be configured and positive")
        if not self.batch_size or self.batch_size <= 0:
            raise ValueError("PRODUCTION: FINBERT_BATCH_SIZE must be configured and positive")

        # Model components
        self.model = None
        self.tokenizer = None
        self.initialized = False

        # Initialize logger IMMEDIATELY - CRITICAL: Must be first
        self.logger = logging.getLogger("enhanced_finbert")
        self.logger.info("FinBERT logger initialized successfully")

        # Performance tracking
        self.total_texts_processed = 0
        self.total_processing_time = 0.0
        self.error_count = 0

        # PRODUCTION: Memory-aware intelligent caching system
        try:
            from Friren_V1.infrastructure.memory_aware_cache import create_memory_aware_cache
            from Friren_V1.infrastructure.configuration_manager import get_config
            
            # Determine environment for cache optimization
            environment = get_config('FRIREN_ENVIRONMENT', 'production')
            if environment == 'development':
                cache_env = 'development'
            elif any(keyword in str(get_config('MEMORY_THRESHOLD_MB', 2000)) for keyword in ['700', '800', '900']):
                cache_env = 't3.micro'  # Detect t3.micro by low memory threshold
            else:
                cache_env = 'production'
                
            self.cache = create_memory_aware_cache("finbert", cache_env)
            self.enable_caching = True
            self.logger.info(f"Initialized memory-aware cache for {cache_env} environment")
            
        except ImportError:
            # Fallback to basic caching if memory-aware cache not available
            self.cache = {}
            self.enable_caching = True
            self.logger.warning("Memory-aware cache not available, using basic caching")

        # Configuration from config manager
        self.confidence_threshold = finbert_config['confidence_threshold']

        # Initialize API resilience manager
        self.resilience_manager = get_resilience_manager() if HAS_RESILIENCE else None

        if HAS_RESILIENCE:
            self.logger.info(f"Enhanced FinBERT initialized with resilience protection - model: {self.model_name}, device: {device}")
        else:
            self.logger.info(f"Enhanced FinBERT initialized (basic error handling) - model: {self.model_name}, device: {device}")

    def initialize(self) -> bool:
        """
        Initialize FinBERT model with resilience protection

        Returns:
            bool: True if successful, False if fallback to mock
        """
        def _initialize_model():
            """Internal function to initialize model (wrapped by resilience system)"""
            self.logger.info("Loading FinBERT model...")

            # Import here to avoid loading unless needed
            import torch
            from transformers import AutoTokenizer, AutoModelForSequenceClassification

            # Load tokenizer
            tokenizer = AutoTokenizer.from_pretrained(self.model_name)

            # Load model
            model = AutoModelForSequenceClassification.from_pretrained(self.model_name)

            # Move to device and set evaluation mode
            model.to(self.device)
            model.eval()

            return tokenizer, model

        try:
            if self.resilience_manager:
                self.tokenizer, self.model = self.resilience_manager.resilient_call(
                    _initialize_model, APIServiceType.FINBERT, "initialize_model",
                    use_circuit_breaker=True, use_retry=True
                )
            else:
                self.tokenizer, self.model = _initialize_model()

            self.initialized = True
            self.logger.info(f"FinBERT model loaded successfully on {self.device} with resilience protection")

            # Test with a sample text to ensure everything works
            test_result = self.analyze_text("The market is performing well today.")
            self.logger.info(f"Model test successful: {test_result.classification}")

            return True

        except ImportError as e:
            self.logger.error(f"CRITICAL: Required libraries not available: {e}")
            self.logger.error("PRODUCTION: Install transformers and torch: pip install transformers torch")
            raise ImportError(f"FinBERT dependencies missing: {e}")

        except Exception as e:
            self.logger.error(f"CRITICAL: Failed to initialize FinBERT model: {e}")
            self.logger.error("PRODUCTION: Check model access and internet connectivity")
            raise RuntimeError(f"FinBERT initialization failed: {e}")

    def analyze_text(self, text: str, article_id: str = None) -> SentimentResult:
        """
        Analyze sentiment of a single text

        Args:
            text: Text to analyze
            article_id: Optional identifier for the text

        Returns:
            SentimentResult with sentiment analysis
        """
        start_time = time.time()

        # Generate article ID if not provided
        if article_id is None:
            article_id = f"text_{hash(text) % 10000}"

        # Check memory-aware cache first
        if self.enable_caching:
            cache_key = self._generate_cache_key(text)
            
            # Try to get from memory-aware cache
            if hasattr(self.cache, 'get'):
                cached_result = self.cache.get(cache_key)
            else:
                # Fallback to simple dict cache
                cached_result = self.cache.get(text)
            
            if cached_result:
                self.logger.debug(f"Cache hit for article {article_id}")
                
                # Handle both new cache format and old format
                if isinstance(cached_result, dict):
                    return SentimentResult(
                        article_id=article_id,
                        text=text,
                        sentiment_score=cached_result['sentiment_score'],
                        confidence=cached_result['confidence'],
                        classification=cached_result['classification'],
                        raw_scores=cached_result['raw_scores'],
                        processing_time=0.001,  # Cache access time
                        model_version=self.model_name,
                        is_reliable=cached_result['is_reliable']
                    )
                else:
                    # Direct SentimentResult object (advanced cache)
                    cached_result.article_id = article_id
                    cached_result.processing_time = 0.001
                    return cached_result

        try:
            if not self.initialized:
                raise RuntimeError("FinBERT model not initialized - call initialize() first")
            
            result = self._analyze_with_finbert(text, article_id)

            # Cache the result with memory-aware caching
            if self.enable_caching:
                self._cache_result(text, result)

            # Update statistics
            processing_time = time.time() - start_time
            self.total_texts_processed += 1
            self.total_processing_time += processing_time

            return result

        except Exception as e:
            self.logger.error(f"CRITICAL: Error analyzing text: {e}")
            self.error_count += 1
            # NO fallback - system must fail if FinBERT cannot analyze
            raise RuntimeError(f"FinBERT analysis failed for text: {e}")

    def analyze_batch(self, texts: List[str],
                      article_ids: Optional[List[str]] = None) -> BatchSentimentResult:
        """
        Analyze sentiment of multiple texts in batch for efficiency

        Args:
            texts: List of texts to analyze
            article_ids: Optional list of identifiers for each text

        Returns:
            BatchSentimentResult with all analysis results
        """
        start_time = time.time()

        if not texts:
            return BatchSentimentResult(
                results=[],
                batch_processing_time=0.0,
                success_count=0,
                error_count=0,
                average_confidence=0.0,
                sentiment_distribution={'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}
            )

        # Generate article IDs if not provided
        if article_ids is None:
            article_ids = [f"batch_{i}_{hash(text) % 10000}" for i, text in enumerate(texts)]

        self.logger.info(f"Analyzing batch of {len(texts)} texts")

        try:
            if self.initialized and len(texts) > 1:
                # Use efficient batch processing
                results = self._analyze_batch_with_finbert(texts, article_ids)
            else:
                # Process individually (for mock or single items)
                results = []
                for text, article_id in zip(texts, article_ids):
                    result = self.analyze_text(text, article_id)
                    results.append(result)

            # Calculate batch statistics
            batch_time = time.time() - start_time
            success_count = len([r for r in results if r.sentiment_score is not None])
            error_count = len(results) - success_count

            confidences = [r.confidence for r in results if r.confidence is not None]
            avg_confidence = np.mean(confidences) if confidences else 0.0

            # Calculate sentiment distribution
            distribution = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}
            for result in results:
                if result.classification in distribution:
                    distribution[result.classification] += 1

            self.logger.info(f"Batch analysis complete - {batch_time:.2f}s, "
                           f"{success_count}/{len(texts)} successful")

            return BatchSentimentResult(
                results=results,
                batch_processing_time=batch_time,
                success_count=success_count,
                error_count=error_count,
                average_confidence=avg_confidence,
                sentiment_distribution=distribution
            )

        except Exception as e:
            self.logger.error(f"CRITICAL: Error in batch analysis: {e}")
            self.error_count += len(texts)
            # NO fallback - system must fail if FinBERT batch processing fails
            raise RuntimeError(f"FinBERT batch analysis failed: {e}")

    def _analyze_with_finbert(self, text: str, article_id: str) -> SentimentResult:
        """Analyze text using actual FinBERT model"""
        import torch

        start_time = time.time()

        # Clean and prepare text
        cleaned_text = self._clean_text(text)

        # Tokenize
        inputs = self.tokenizer(
            cleaned_text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=self.max_length
        )

        # Move to device
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # Run inference
        with torch.no_grad():
            outputs = self.model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

        # Calculate processing time
        processing_time = time.time() - start_time

        # Extract scores
        scores = predictions[0].cpu().numpy()

        # FinBERT outputs: [negative, neutral, positive]
        negative_score = float(scores[0])
        neutral_score = float(scores[1])
        positive_score = float(scores[2])

        # Calculate overall sentiment (-1 to +1)
        sentiment_score = positive_score - negative_score

        # Classification based on highest score
        max_idx = np.argmax(scores)
        classifications = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
        classification = classifications[max_idx]

        # Confidence is the max probability
        confidence = float(np.max(scores))

        # Check reliability based on confidence threshold
        is_reliable = confidence >= self.confidence_threshold

        # Raw scores for debugging/analysis
        raw_scores = {
            'negative': negative_score,
            'neutral': neutral_score,
            'positive': positive_score,
            'max_score': confidence,
            'prediction_index': int(max_idx)
        }

        # BUSINESS LOGIC OUTPUT: Detailed FinBERT analysis result using OutputCoordinator
        try:
            # Use OutputCoordinator for consistent business logic output
            from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator
            symbol_hint = self._extract_symbol_from_article_id(article_id)
            article_preview = text[:40] + '...' if len(text) > 40 else text
            impact_score = abs(sentiment_score) * confidence  # Combined impact metric
            
            # Get or create OutputCoordinator instance (would be passed from parent in production)
            if not hasattr(self, '_output_coordinator'):
                self._output_coordinator = OutputCoordinator()
            
            # Use standardized FinBERT analysis output
            self._output_coordinator.output_finbert_analysis(
                symbol=symbol_hint,
                sentiment=classification,
                confidence=confidence*100,
                article_snippet=article_preview,
                impact=impact_score
            )
            
        except ImportError:
            # Fallback to direct output only if OutputCoordinator not available
            symbol_hint = self._extract_symbol_from_article_id(article_id)
            article_preview = text[:40] + '...' if len(text) > 40 else text
            impact_score = abs(sentiment_score) * confidence  # Combined impact metric
            print(f"[FINBERT] {symbol_hint}: {classification} (confidence: {confidence*100:.1f}%) | article: '{article_preview}' | impact: {impact_score:.2f}")

        return SentimentResult(
            article_id=article_id,
            text=text,
            sentiment_score=sentiment_score,
            confidence=confidence,
            classification=classification,
            raw_scores=raw_scores,
            processing_time=processing_time,
            model_version=self.model_name,
            is_reliable=is_reliable
        )

    def _analyze_batch_with_finbert(self, texts: List[str], article_ids: List[str]) -> List[SentimentResult]:
        """Efficiently analyze multiple texts using FinBERT"""
        import torch

        start_time = time.time()

        # Clean all texts
        cleaned_texts = [self._clean_text(text) for text in texts]

        # Tokenize batch
        inputs = self.tokenizer(
            cleaned_texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=self.max_length
        )

        # Move to device
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # Run batch inference
        with torch.no_grad():
            outputs = self.model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

        # Calculate batch processing time
        batch_processing_time = time.time() - start_time
        individual_processing_time = batch_processing_time / len(texts)

        # Process results
        results = []
        for i, (text, article_id) in enumerate(zip(texts, article_ids)):
            scores = predictions[i].cpu().numpy()

            # FinBERT outputs: [negative, neutral, positive]
            negative_score = float(scores[0])
            neutral_score = float(scores[1])
            positive_score = float(scores[2])

            # Calculate overall sentiment (-1 to +1)
            sentiment_score = positive_score - negative_score

            # Classification based on highest score
            max_idx = np.argmax(scores)
            classifications = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
            classification = classifications[max_idx]

            # Confidence is the max probability
            confidence = float(np.max(scores))

            # Check reliability based on confidence threshold
            is_reliable = confidence >= self.confidence_threshold

            # Raw scores
            raw_scores = {
                'negative': negative_score,
                'neutral': neutral_score,
                'positive': positive_score,
                'max_score': confidence,
                'prediction_index': int(max_idx)
            }

            result = SentimentResult(
                article_id=article_id,
                text=text,
                sentiment_score=sentiment_score,
                confidence=confidence,
                classification=classification,
                raw_scores=raw_scores,
                processing_time=individual_processing_time,
                model_version=self.model_name,
                is_reliable=is_reliable
            )

            results.append(result)

        return results

    # REMOVED: Mock implementation completely eliminated per user requirements
    # NO mock data allowed - system must use real FinBERT or fail

    def _clean_text(self, text: str) -> str:
        """Clean and prepare text for analysis"""
        if not text:
            return ""

        # Remove excessive whitespace
        text = " ".join(text.split())

        # Remove very short texts - FROM CONFIGURATION MANAGER
        min_text_length = get_config('MIN_TEXT_LENGTH', 10)
        if len(text.strip()) < min_text_length:
            return text

        # Truncate if too long (leave room for tokenization) - FROM CONFIGURATION MANAGER
        chars_per_token_ratio = get_config('FINBERT_CHARS_PER_TOKEN_RATIO', 3)
        max_chars = self.max_length * chars_per_token_ratio  # Estimate from configuration
        if len(text) > max_chars:
            text = text[:max_chars] + "..."

        return text.strip()

    def _generate_cache_key(self, text: str) -> str:
        """Generate optimized cache key for text"""
        import hashlib
        
        # Normalize text for consistent caching
        normalized_text = text.strip().lower()
        
        # For very short texts, use the text itself
        if len(normalized_text) <= 50:
            return f"short_{normalized_text}"
        
        # For longer texts, use a hash to save memory
        text_hash = hashlib.md5(normalized_text.encode('utf-8')).hexdigest()
        return f"hash_{text_hash}_{len(text)}"
    
    def _cache_result(self, text: str, result: SentimentResult):
        """Cache result using memory-aware caching system"""
        try:
            cache_key = self._generate_cache_key(text)
            
            if hasattr(self.cache, 'put'):
                # Use memory-aware cache with intelligent storage
                cache_data = {
                    'sentiment_score': result.sentiment_score,
                    'confidence': result.confidence,
                    'classification': result.classification,
                    'raw_scores': result.raw_scores,
                    'is_reliable': result.is_reliable,
                    'model_version': result.model_version,
                    'cached_at': datetime.now().isoformat()
                }
                
                # Cache with TTL based on confidence (higher confidence = longer TTL)
                confidence_ttl = int(1800 + (result.confidence * 1800))  # 30-60 minutes based on confidence
                
                success = self.cache.put(cache_key, cache_data, ttl_seconds=confidence_ttl)
                if success:
                    self.logger.debug(f"Cached result with key: {cache_key[:20]}... (TTL: {confidence_ttl}s)")
                else:
                    self.logger.warning(f"Failed to cache result - cache may be full")
                    
            else:
                # Fallback to simple dict cache - FROM CONFIGURATION MANAGER
                cache_size_limit = get_config('FINBERT_CACHE_SIZE_LIMIT', 1000)
                cache_cleanup_count = get_config('FINBERT_CACHE_CLEANUP_COUNT', 250)
                if len(self.cache) >= cache_size_limit:  # Size limit from configuration
                    # Remove entries from configuration
                    keys_to_remove = list(self.cache.keys())[:cache_cleanup_count]
                    for key in keys_to_remove:
                        del self.cache[key]
                
                self.cache[text] = {
                    'sentiment_score': result.sentiment_score,
                    'confidence': result.confidence,
                    'classification': result.classification,
                    'raw_scores': result.raw_scores,
                    'is_reliable': result.is_reliable
                }
                
        except Exception as e:
            self.logger.error(f"Error caching result: {e}")
            # Continue without caching rather than fail

    # REMOVED: Fallback result method completely eliminated per user requirements
    # NO fallback data allowed - system must use real FinBERT or fail

    def get_statistics(self) -> Dict[str, Any]:
        """Get performance and usage statistics with memory-aware cache details"""
        avg_processing_time = (self.total_processing_time / self.total_texts_processed
                             if self.total_texts_processed > 0 else 0.0)

        # Get cache statistics from memory-aware cache or fallback
        cache_stats = {}
        if hasattr(self.cache, 'get_statistics'):
            cache_stats = self.cache.get_statistics()
            cache_size = cache_stats.get('current_entry_count', 0)
        else:
            cache_size = len(self.cache)

        return {
            'initialized': self.initialized,
            'model_name': self.model_name,
            'device': self.device,
            'total_texts_processed': self.total_texts_processed,
            'total_processing_time': self.total_processing_time,
            'average_processing_time': avg_processing_time,
            'error_count': self.error_count,
            'error_rate': self.error_count / max(1, self.total_texts_processed),
            'cache_size': cache_size,
            'cache_enabled': self.enable_caching,
            'confidence_threshold': self.confidence_threshold,
            'reliability_enforcement': True,
            'processing_time_tracking': True,
            'memory_aware_caching': hasattr(self.cache, 'get_statistics'),
            'cache_statistics': cache_stats
        }

    def clear_cache(self):
        """Clear the result cache using memory-aware cache system"""
        if hasattr(self.cache, 'clear'):
            self.cache.clear()
        else:
            # Fallback to dict clear
            self.cache.clear()
        self.logger.info("FinBERT cache cleared")

    def set_confidence_threshold(self, threshold: float):
        """Set minimum confidence threshold for reliable results"""
        self.confidence_threshold = max(0.0, min(1.0, threshold))
        self.logger.info(f"Confidence threshold set to {self.confidence_threshold}")

    def _extract_symbol_from_article_id(self, article_id: str, fallback_symbol: str = None) -> str:
        """
        Robustly extract symbol from article_id with multiple fallback strategies
        
        Args:
            article_id: Article identifier (e.g., "AAPL_1234", "text_5678")
            fallback_symbol: Optional fallback symbol if extraction fails
            
        Returns:
            Extracted symbol or fallback
        """
        if not article_id:
            return fallback_symbol or 'UNKNOWN'
            
        # Strategy 1: Standard format "SYMBOL_id"
        if '_' in article_id:
            potential_symbol = article_id.split('_')[0].upper()
            
            # Special case: If it's "text_xxxx" format, this is from fallback generation
            if potential_symbol == 'TEXT':
                return fallback_symbol or 'UNKNOWN'
                
            # Validate it looks like a stock symbol - FROM CONFIGURATION MANAGER
            # Validate symbol length from configuration\n            min_symbol_length = get_config('MIN_SYMBOL_LENGTH', 2)\n            max_symbol_length = get_config('MAX_SYMBOL_LENGTH', 5)\n            if min_symbol_length <= len(potential_symbol) <= max_symbol_length and potential_symbol.isalpha():
                return potential_symbol
        
        # Strategy 2: Look for common stock patterns in the ID
        import re
        # Match 2-5 letter sequences that look like stock symbols
        stock_pattern = re.findall(r'\b[A-Z]{2,5}\b', article_id.upper())
        if stock_pattern:
            return stock_pattern[0]
            
        # Strategy 3: Return fallback or UNKNOWN
        return fallback_symbol or 'UNKNOWN'

    def _validate_symbol_consistency(self, expected_symbol: str, extracted_symbol: str, article_id: str):
        """Log symbol extraction issues for debugging"""
        if expected_symbol and expected_symbol.upper() != extracted_symbol.upper():
            self.logger.warning(f"SYMBOL MISMATCH: Expected {expected_symbol}, extracted {extracted_symbol} from article_id {article_id}")

    def cleanup(self):
        """Cleanup FinBERT resources and memory with memory-aware cache management"""
        try:
            if hasattr(self, 'model') and self.model is not None:
                # Clear model from memory
                del self.model
                self.model = None
                
            if hasattr(self, 'tokenizer') and self.tokenizer is not None:
                # Clear tokenizer from memory
                del self.tokenizer
                self.tokenizer = None
                
            # Clear cache using memory-aware cache cleanup
            if hasattr(self, 'cache'):
                if hasattr(self.cache, 'cleanup'):
                    # Use memory-aware cache cleanup (stops monitoring, clears entries)
                    self.cache.cleanup()
                else:
                    # Fallback to simple clear
                    self.cache.clear()
                
            # Force garbage collection for memory cleanup
            import gc
            gc.collect()
            
            # Reset initialization state
            self.initialized = False
            
            self.logger.info("FinBERT cleanup completed successfully with memory-aware cache management")
            
        except Exception as e:
            self.logger.warning(f"Error during FinBERT cleanup: {e}")


# Example usage and testing
if __name__ == "__main__":
    # Test the FinBERT analyzer
    finbert = EnhancedFinBERT()

    # Initialize (will fall back to mock if FinBERT not available)
    success = finbert.initialize()
    print(f"FinBERT initialization: {'Success' if success else 'Mock mode'}")

    # Test single text analysis
    test_text = "Apple reported strong quarterly earnings, beating analyst expectations with robust iPhone sales driving revenue growth."
    result = finbert.analyze_text(test_text)

    print(f"\nSingle Text Analysis:")
    print(f"Text: {test_text[:100]}...")
    print(f"Sentiment: {result.classification} (score: {result.sentiment_score:.3f})")
    print(f"Confidence: {result.confidence:.3f}")
    print(f"Processing time: {result.processing_time:.4f}s")

    # Test batch analysis
    test_texts = [
        "Tesla stock surges on strong delivery numbers and positive guidance",
        "Market volatility continues amid economic uncertainty and inflation concerns",
        "Microsoft announces disappointing quarterly results missing revenue expectations",
        "Federal Reserve signals potential interest rate cuts supporting market sentiment"
    ]

    batch_result = finbert.analyze_batch(test_texts)

    print(f"\nBatch Analysis ({len(test_texts)} texts):")
    print(f"Processing time: {batch_result.batch_processing_time:.3f}s")
    print(f"Success rate: {batch_result.success_count}/{len(test_texts)}")
    print(f"Average confidence: {batch_result.average_confidence:.3f}")
    print(f"Distribution: {batch_result.sentiment_distribution}")

    for i, result in enumerate(batch_result.results):
        print(f"  {i+1}. {result.classification} ({result.sentiment_score:+.3f}) "
              f"[{result.confidence:.3f}] {'' if result.is_reliable else ''} - {test_texts[i][:60]}...")

    # Show statistics
    stats = finbert.get_statistics()
    print(f"\nStatistics:")
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")

    # Test confidence threshold
    print(f"\nConfidence Threshold Analysis:")
    reliable_count = sum(1 for r in batch_result.results if r.is_reliable)
    print(f"Reliable results: {reliable_count}/{len(batch_result.results)} "
          f"(threshold: {finbert.confidence_threshold:.2f})")

    # Show processing time breakdown
    total_time = sum(r.processing_time for r in batch_result.results)
    print(f"Total processing time: {total_time:.4f}s")
    print(f"Average per text: {total_time/len(batch_result.results):.4f}s")

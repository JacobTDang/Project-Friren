"""
Targeted News Messaging System

Event-driven messaging for chaos detection → targeted news collection → decision engine

This module handles the communication flow:
Strategy Analyzer (chaos detected) → Decision Engine (needs data) → Targeted Collector → Decision Engine (enhanced decision)
"""

import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum

# Import Redis messaging system
try:
    from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
        get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
    )
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Import targeted news collector (now professional multi-source version)
try:
    from Friren_V1.trading_engine.data.professional_targeted_news_collector import (
        TargetedNewsRequest, TargetedNewsResponse, get_professional_targeted_collector, NewsSource
    )
    COLLECTOR_AVAILABLE = True
except ImportError:
    # Fallback to original Yahoo-only collector
    try:
        from Friren_V1.trading_engine.data.targeted_news_collector import (
            TargetedNewsRequest, TargetedNewsResponse, get_targeted_collector as get_professional_targeted_collector
        )
        COLLECTOR_AVAILABLE = True
    except ImportError:
        COLLECTOR_AVAILABLE = False


class ChaosEventType(Enum):
    """Types of chaos events that trigger targeted news collection"""
    HIGH_ENTROPY = "high_entropy"
    VOLATILITY_SPIKE = "volatility_spike"
    REGIME_UNCERTAINTY = "regime_uncertainty"
    TRANSITION_INSTABILITY = "transition_instability"
    STRATEGY_CONFLICT = "strategy_conflict"
    MARKET_ANOMALY = "market_anomaly"


@dataclass
class ChaosAlert:
    """Chaos detection alert from Strategy Analyzer"""
    alert_id: str
    symbol: str
    chaos_type: ChaosEventType
    chaos_level: str  # low, medium, high, critical
    urgency: str     # low, medium, high, critical
    confidence: float
    detection_time: datetime
    regime_context: Dict[str, Any]
    recommended_action: str
    source_process: str = "strategy_analyzer"
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['chaos_type'] = self.chaos_type.value
        result['detection_time'] = self.detection_time.isoformat()
        return result


@dataclass 
class NewsRequestMessage:
    """Request for targeted news collection"""
    request_id: str
    symbol: str
    trigger_alert: ChaosAlert
    priority: str  # low, medium, high, critical
    max_articles: int
    hours_back: int
    requesting_process: str
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['trigger_alert'] = self.trigger_alert.to_dict()
        result['timestamp'] = self.timestamp.isoformat()
        return result


@dataclass
class EnhancedDecisionContext:
    """Enhanced context for decision engine with targeted news"""
    symbol: str
    chaos_alert: ChaosAlert
    news_response: TargetedNewsResponse
    analysis_timestamp: datetime
    context_quality: str  # poor, good, excellent
    recommendation_confidence_boost: float  # 0.0 to 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['chaos_alert'] = self.chaos_alert.to_dict()
        result['analysis_timestamp'] = self.analysis_timestamp.isoformat()
        # Convert news_response to dict
        result['news_response'] = {
            'request_id': self.news_response.request_id,
            'symbol': self.news_response.symbol,
            'articles_count': len(self.news_response.articles),
            'collection_time': self.news_response.collection_time.isoformat(),
            'processing_time_ms': self.news_response.processing_time_ms,
            'success': self.news_response.success,
            'articles': [
                {
                    'title': article.title,
                    'source': article.source,
                    'url': article.url,
                    'published_date': article.published_date.isoformat() if article.published_date else None
                }
                for article in self.news_response.articles
            ]
        }
        return result


class TargetedNewsOrchestrator:
    """
    Orchestrates the event-driven targeted news collection flow
    
    Handles the complete workflow:
    1. Receives chaos alerts from Strategy Analyzer
    2. Evaluates need for targeted news collection
    3. Requests targeted news collection
    4. Provides enhanced context to Decision Engine
    """
    
    def __init__(self):
        self.logger = logging.getLogger("targeted_news_orchestrator")
        self.redis_manager = get_trading_redis_manager() if REDIS_AVAILABLE else None
        self.targeted_collector = get_professional_targeted_collector() if COLLECTOR_AVAILABLE else None
        
        # Tracking
        self.active_requests = {}  # request_id -> NewsRequestMessage
        self.processed_alerts = []  # Recent chaos alerts processed
        self.performance_metrics = {
            'alerts_received': 0,
            'news_requests_made': 0,
            'successful_collections': 0,
            'avg_response_time_ms': 0.0,
            'context_enhancements_provided': 0
        }
        
        # Configuration
        self.max_concurrent_requests = 5
        self.min_time_between_requests = 60  # seconds
        self.last_request_times = {}  # symbol -> timestamp
        
        self.logger.info("TargetedNewsOrchestrator initialized")
    
    def process_chaos_alert(self, chaos_alert: ChaosAlert) -> Optional[str]:
        """
        Process chaos alert from Strategy Analyzer
        
        Returns request_id if news collection was triggered, None otherwise
        """
        try:
            self.logger.info(f"CHAOS ALERT: {chaos_alert.symbol} - {chaos_alert.chaos_level} {chaos_alert.chaos_type.value}")
            
            self.performance_metrics['alerts_received'] += 1
            self._record_processed_alert(chaos_alert)
            
            # Evaluate if we should trigger targeted news collection
            if not self._should_trigger_news_collection(chaos_alert):
                self.logger.debug(f"Skipping news collection for {chaos_alert.symbol} - criteria not met")
                return None
            
            # Create targeted news request
            request = self._create_news_request(chaos_alert)
            
            # Execute targeted news collection
            response = self._execute_news_collection(request)
            
            if response and response.success:
                # Provide enhanced context to decision engine
                self._provide_enhanced_context(chaos_alert, response)
                self.logger.info(f"TARGETED NEWS SUCCESS: {chaos_alert.symbol} - {len(response.articles)} articles collected")
                return request.request_id
            else:
                self.logger.warning(f"TARGETED NEWS FAILED: {chaos_alert.symbol}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error processing chaos alert for {chaos_alert.symbol}: {e}")
            return None
    
    def _should_trigger_news_collection(self, chaos_alert: ChaosAlert) -> bool:
        """Evaluate if chaos alert warrants targeted news collection"""
        
        # Check urgency threshold
        if chaos_alert.urgency not in ['medium', 'high', 'critical']:
            return False
        
        # Check if we're at max concurrent requests
        if len(self.active_requests) >= self.max_concurrent_requests:
            self.logger.warning("Max concurrent news requests reached, skipping collection")
            return False
        
        # Check minimum time between requests for same symbol
        symbol = chaos_alert.symbol
        if symbol in self.last_request_times:
            time_since_last = (datetime.now() - self.last_request_times[symbol]).total_seconds()
            if time_since_last < self.min_time_between_requests:
                self.logger.debug(f"Too soon for another request for {symbol} ({time_since_last:.1f}s)")
                return False
        
        # Check chaos confidence threshold
        if chaos_alert.confidence < 0.6:  # 60% minimum confidence
            return False
        
        return True
    
    def _create_news_request(self, chaos_alert: ChaosAlert) -> NewsRequestMessage:
        """Create targeted news request from chaos alert"""
        request_id = f"chaos_{chaos_alert.symbol}_{int(time.time())}"
        
        # Determine collection parameters based on urgency
        if chaos_alert.urgency == 'critical':
            max_articles = 8
            hours_back = 4
        elif chaos_alert.urgency == 'high':
            max_articles = 6
            hours_back = 6
        elif chaos_alert.urgency == 'medium':
            max_articles = 4
            hours_back = 8
        else:
            max_articles = 3
            hours_back = 12
        
        return NewsRequestMessage(
            request_id=request_id,
            symbol=chaos_alert.symbol,
            trigger_alert=chaos_alert,
            priority=chaos_alert.urgency,
            max_articles=max_articles,
            hours_back=hours_back,
            requesting_process="targeted_news_orchestrator",
            timestamp=datetime.now()
        )
    
    def _execute_news_collection(self, request: NewsRequestMessage) -> Optional[TargetedNewsResponse]:
        """Execute targeted news collection"""
        if not self.targeted_collector:
            self.logger.error("Targeted collector not available")
            return None
        
        try:
            # Track active request
            self.active_requests[request.request_id] = request
            self.last_request_times[request.symbol] = datetime.now()
            
            # Create collection request
            collection_request = TargetedNewsRequest(
                symbol=request.symbol,
                trigger_reason=request.trigger_alert.chaos_type.value,
                urgency=request.priority,
                max_articles=request.max_articles,
                hours_back=request.hours_back,
                request_id=request.request_id
            )
            
            # Execute collection
            start_time = time.time()
            response = self.targeted_collector.collect_targeted_news(collection_request)
            collection_time = (time.time() - start_time) * 1000
            
            # Update metrics
            self.performance_metrics['news_requests_made'] += 1
            if response.success:
                self.performance_metrics['successful_collections'] += 1
            
            # Update average response time
            if self.performance_metrics['avg_response_time_ms'] == 0:
                self.performance_metrics['avg_response_time_ms'] = collection_time
            else:
                self.performance_metrics['avg_response_time_ms'] = (
                    self.performance_metrics['avg_response_time_ms'] * 0.8 + collection_time * 0.2
                )
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error executing news collection for {request.symbol}: {e}")
            return None
        finally:
            # Clean up active request
            if request.request_id in self.active_requests:
                del self.active_requests[request.request_id]
    
    def _provide_enhanced_context(self, chaos_alert: ChaosAlert, news_response: TargetedNewsResponse):
        """Provide enhanced context to decision engine"""
        try:
            # Create enhanced context
            context = EnhancedDecisionContext(
                symbol=chaos_alert.symbol,
                chaos_alert=chaos_alert,
                news_response=news_response,
                analysis_timestamp=datetime.now(),
                context_quality=self._assess_context_quality(news_response),
                recommendation_confidence_boost=self._calculate_confidence_boost(chaos_alert, news_response)
            )
            
            # Send to decision engine via Redis
            if self.redis_manager:
                decision_message = create_process_message(
                    sender="targeted_news_orchestrator",
                    recipient="decision_engine",
                    message_type="ENHANCED_CONTEXT",
                    data=context.to_dict(),
                    priority=MessagePriority.HIGH if chaos_alert.urgency == 'critical' else MessagePriority.MEDIUM
                )
                
                self.redis_manager.send_message(decision_message)
                self.performance_metrics['context_enhancements_provided'] += 1
                
                self.logger.info(f"ENHANCED CONTEXT: Sent to decision engine for {chaos_alert.symbol}")
            
        except Exception as e:
            self.logger.error(f"Error providing enhanced context for {chaos_alert.symbol}: {e}")
    
    def _assess_context_quality(self, news_response: TargetedNewsResponse) -> str:
        """Assess quality of collected news context"""
        if not news_response.success:
            return "poor"
        
        article_count = len(news_response.articles)
        if article_count >= 5:
            return "excellent"
        elif article_count >= 3:
            return "good"
        else:
            return "poor"
    
    def _calculate_confidence_boost(self, chaos_alert: ChaosAlert, news_response: TargetedNewsResponse) -> float:
        """Calculate confidence boost for decision engine based on context quality"""
        if not news_response.success:
            return 0.0
        
        base_boost = 0.1  # 10% base boost for having news context
        
        # Article count boost
        article_count = len(news_response.articles)
        article_boost = min(article_count * 0.05, 0.2)  # Up to 20% boost
        
        # Urgency boost
        urgency_boosts = {'low': 0.0, 'medium': 0.05, 'high': 0.1, 'critical': 0.15}
        urgency_boost = urgency_boosts.get(chaos_alert.urgency, 0.0)
        
        # Speed boost (faster collection = more timely context)
        speed_boost = 0.05 if news_response.processing_time_ms < 3000 else 0.0
        
        total_boost = min(base_boost + article_boost + urgency_boost + speed_boost, 0.4)  # Cap at 40%
        return total_boost
    
    def _record_processed_alert(self, chaos_alert: ChaosAlert):
        """Record processed chaos alert for tracking"""
        self.processed_alerts.append(chaos_alert)
        
        # Keep only last 100 alerts
        if len(self.processed_alerts) > 100:
            self.processed_alerts = self.processed_alerts[-100:]
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for monitoring"""
        return {
            **self.performance_metrics,
            'active_requests': len(self.active_requests),
            'recent_alerts': len([a for a in self.processed_alerts 
                                 if (datetime.now() - a.detection_time).total_seconds() < 3600]),
            'collector_available': COLLECTOR_AVAILABLE,
            'redis_available': REDIS_AVAILABLE
        }


# Global orchestrator instance
_orchestrator = None

def get_targeted_news_orchestrator() -> TargetedNewsOrchestrator:
    """Get global targeted news orchestrator instance"""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = TargetedNewsOrchestrator()
    return _orchestrator


def send_chaos_alert(chaos_indicators: Dict[str, Any], source_process: str = "strategy_analyzer") -> Optional[str]:
    """
    Convenience function to send chaos alert from Strategy Analyzer
    
    Args:
        chaos_indicators: Result from StrategyAnalyzer.detect_market_chaos()
        source_process: Process sending the alert
    
    Returns:
        request_id if news collection was triggered, None otherwise
    """
    if not chaos_indicators.get('chaos_detected', False):
        return None
    
    try:
        # Convert chaos indicators to ChaosAlert
        chaos_type_map = {
            'high_entropy': ChaosEventType.HIGH_ENTROPY,
            'volatility_spike': ChaosEventType.VOLATILITY_SPIKE,
            'low_confidence': ChaosEventType.REGIME_UNCERTAINTY,
            'high_transition_prob': ChaosEventType.TRANSITION_INSTABILITY,
            'regime_detector_conflict': ChaosEventType.STRATEGY_CONFLICT
        }
        
        primary_reason = chaos_indicators.get('primary_reason', '')
        chaos_type = ChaosEventType.MARKET_ANOMALY  # default
        
        for reason_key, event_type in chaos_type_map.items():
            if reason_key in primary_reason:
                chaos_type = event_type
                break
        
        chaos_alert = ChaosAlert(
            alert_id=f"alert_{chaos_indicators['symbol']}_{int(time.time())}",
            symbol=chaos_indicators['symbol'],
            chaos_type=chaos_type,
            chaos_level=chaos_indicators.get('chaos_level', 'medium'),
            urgency=chaos_indicators.get('urgency', 'medium'),
            confidence=chaos_indicators.get('chaos_score', 0.5),
            detection_time=datetime.now(),
            regime_context=chaos_indicators.get('regime_analysis', {}),
            recommended_action=chaos_indicators.get('recommendation', 'monitor'),
            source_process=source_process
        )
        
        # Send to orchestrator
        orchestrator = get_targeted_news_orchestrator()
        return orchestrator.process_chaos_alert(chaos_alert)
        
    except Exception as e:
        logging.getLogger("targeted_news_messaging").error(f"Error sending chaos alert: {e}")
        return None
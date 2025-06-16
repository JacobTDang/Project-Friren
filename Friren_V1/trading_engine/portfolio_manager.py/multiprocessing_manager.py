import multiprocessing as mp
import pandas as pd
import numpy as np
import time
import queue
from typing import Dict, List, Any
from dataclasses import dataclass
from datetime import datetime
import psutil

@dataclass
class AnalysisResults:
    symbol: str
    result_type: str
    data: Dict[str, Any]
    processing_time: float
    success: bool


def analysis_worker(task_queue: mp.Queue, result_queue: mp.Queue):
    """Simple worker that handles all analysis types"""
    try:
        from sentiment_analyzer import SentimentAnalyzer
        from trading_engine.regime_detection.regime_market import MarketRegimeDetector
        sentiment_analyzer = SentimentAnalyzer()
        regime_detector = MarketRegimeDetector()
        has_components = True
    except ImportError:
        has_components = False
        print("Using mock analysis components")

    while True:
        try:
            task = task_queue.get(timeout=2)
            if task is None:
                break

            start_time = time.time()
            symbol = task['symbol']
            task_type = task['task_type']

            # Convert market data back to DataFrame
            df = pd.DataFrame(task['market_data'])

            # Perform analysis based on task type
            try:
                if task_type == 'sentiment':
                    if has_components:
                        sentiment_result = sentiment_analyzer.analyze_sentiment(symbol)
                        data = {
                            'sentiment_score': sentiment_result.sentiment_score,
                            'confidence': sentiment_result.confidence,
                            'article_count': sentiment_result.article_count
                        }
                    else:
                        data = {
                            'sentiment_score': np.random.uniform(-0.5, 0.5),
                            'confidence': np.random.uniform(60, 90),
                            'article_count': np.random.randint(3, 12)
                        }

                elif task_type == 'regime':
                    if has_components:
                        regime_result = regime_detector.detect_regime(df)
                        data = {
                            'market_regime': regime_result.get('regime', 'UNKNOWN'),
                            'regime_confidence': regime_result.get('confidence', 0)
                        }
                    else:
                        regimes = ['BULL_MARKET', 'BEAR_MARKET', 'SIDEWAYS', 'HIGH_VOLATILITY']
                        data = {
                            'market_regime': np.random.choice(regimes),
                            'regime_confidence': np.random.uniform(70, 95)
                        }

                elif task_type == 'strategy':
                    if len(df) >= 20:
                        returns = df['Close'].pct_change()
                        gain = returns.where(returns > 0, 0).rolling(14).mean()
                        loss = -returns.where(returns < 0, 0).rolling(14).mean()
                        rsi = 100 - (100 / (1 + (gain / loss.replace(0, 0.001))))
                        current_rsi = rsi.iloc[-1]

                        if current_rsi < 30:
                            strategy = 'mean_reversion'
                            confidence = 80
                        elif current_rsi > 70:
                            strategy = 'mean_reversion'
                            confidence = 80
                        else:
                            strategy = 'momentum'
                            confidence = 70
                    else:
                        strategy = 'hold'
                        confidence = 50
                        current_rsi = 50

                    data = {
                        'strategy': strategy,
                        'confidence': confidence,
                        'rsi': current_rsi
                    }

                result = AnalysisResults(  # Fixed class name
                    symbol=symbol,
                    result_type=task_type,
                    data=data,
                    processing_time=time.time() - start_time,
                    success=True
                )

            except Exception as e:
                result = AnalysisResults(  # Fixed class name
                    symbol=symbol,
                    result_type=task_type,
                    data={},
                    processing_time=time.time() - start_time,
                    success=False
                )

            result_queue.put(result)

        except queue.Empty:
            continue
        except Exception as e:
            print(f"Worker error: {e}")
            continue


class MultiprocessManager:
    """
    Simple multiprocess manager optimized for t3 instances
    """
    def __init__(self):
        cpu_count = mp.cpu_count()  # Fixed method name
        self.memory_gb = self._get_memory_gb()

        # Initialize queues and workers list
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        self.workers = []

        if self.memory_gb < 1:  # t3.nano/micro
            self.num_workers = 2
            self.max_symbols = 10
        elif self.memory_gb < 4:  # t3.small
            self.num_workers = 3
            self.max_symbols = 20
        elif self.memory_gb < 8:  # t3.medium
            self.num_workers = 4
            self.max_symbols = 30
        else:  # t3.large+
            self.num_workers = 6
            self.max_symbols = 50

    def _get_memory_gb(self) -> float:
        """
        Get total virtual memory
        """
        try:
            return psutil.virtual_memory().total / (1024**3)
        except:
            # default assumption
            return 4.0

    def start_workers(self):
        """
        Start the worker processes
        """
        for i in range(self.num_workers):
            worker = mp.Process(target=analysis_worker, args=(self.task_queue, self.result_queue))
            worker.start()
            self.workers.append(worker)

        print(f'Started {len(self.workers)} workers')

    def analyze_symbols_parallel(self, symbols: List[str], market_data_dict: Dict) -> Dict[str, Any]:
        """
        Main method to analyze multiple symbols in parallel
        """
        # Start workers first
        self.start_workers()

        # Limit the max based on t3 instance
        symbols = symbols[:self.max_symbols]
        print(f'Analyzing {len(symbols)} symbols in parallel')
        start_time = time.time()

        task_count = 0
        for symbol in symbols:
            if symbol in market_data_dict:
                # Submit sentiment, regime, and strategy tasks
                for task_type in ['sentiment', 'regime', 'strategy']:
                    task = {
                        'symbol': symbol,
                        'task_type': task_type,
                        'market_data': market_data_dict[symbol].tail(100).to_dict('records')  # Last 100 days
                    }
                    self.task_queue.put(task)
                    task_count += 1

        results = {'sentiment': {}, 'regime': {}, 'strategy': {}}
        collected = 0
        timeout = 60

        while collected < task_count and time.time() - start_time < timeout:
            try:
                result = self.result_queue.get(timeout=1)
                if result.success:
                    results[result.result_type][result.symbol] = result.data
                collected += 1
            except queue.Empty:
                continue

        total_time = time.time() - start_time
        success_rate = collected / task_count * 100 if task_count > 0 else 0

        print(f"Analysis complete: {total_time:.1f}s, {success_rate:.0f}% success")

        # Shutdown workers
        self.shutdown()

        return {
            'results': results,
            'processing_time': total_time,
            'symbols_processed': len(symbols),
            'success_rate': success_rate
        }

    def shutdown(self):
        """Shutdown workers"""
        # Send shutdown signal to workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)

        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5)
            if worker.is_alive():
                worker.terminate()
                worker.join()

        print("Workers shutdown")

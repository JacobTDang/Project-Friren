from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline
from news_sentiment import NewsSentimentCollector

class finbert:
  def __init__(self):
    # Load tokenizer and model
    model_name = "yiyanghkust/finbert-tone"
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertForSequenceClassification.from_pretrained(model_name)

    # Create sentiment analysis pipeline
    self.nlp = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

    # Create instance for webscrape importers
    self.collector = NewsSentimentCollector()

  def analyze_articles(self, articles):
    for article in articles:
      if not article['source'].lower.contains("yahoo"):
        self.nlp(article['title'])

  def collect_news(self):
    # Collect news in the last 6 hours
    # will most likely have finBERT run every 6 hours
    try:
      articles = self.collector.collect_all_news(hours_back = 6)
    except Exception as e:
      print(f'Error occured: {e}')

    if articles:
        print(f"\nCollected {len(articles)} articles (titles only)")

    return articles

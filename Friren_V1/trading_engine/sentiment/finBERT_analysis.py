from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline

class finbert:
  def __init__(self):
    # Load tokenizer and model
    model_name = "yiyanghkust/finbert-tone"
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertForSequenceClassification.from_pretrained(model_name)

    # Create sentiment analysis pipeline
    self.nlp = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

  def analyze_text(self, text):
    self.nlp(text)

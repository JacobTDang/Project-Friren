from django.contrib import admin
from .models import TransactionHistory

@admin.register(TransactionHistory)
class TransactionHistoryAdmin(admin.ModelAdmin):
  list_display = ['symbol', 'quantity', 'price', 'timestamp', 'sentiment_score']
  list_filter = ['symbol', 'timestamp']
  search_fields = ['symbol', 'order_id']

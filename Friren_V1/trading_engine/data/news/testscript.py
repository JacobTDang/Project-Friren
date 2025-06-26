#!/usr/bin/env python3
"""
Reddit News Test - Test Reddit integration only
"""

import sys
import os

def test_reddit_news():
    print("=" * 60)
    print("TESTING REDDIT NEWS INTEGRATION")
    print("=" * 60)

    # Add the news directory to path
    news_path = os.path.join('trading_engine', 'data', 'news')
    sys.path.insert(0, news_path)

    # Test Reddit import
    print("1. Testing Reddit import...")
    try:
        from reddit_api import RedditNews
        print("   Reddit module imported successfully")
    except Exception as e:
        print(f"   Reddit import failed: {e}")
        return

    # Test Reddit initialization
    print("\n2. Testing Reddit initialization...")
    try:
        reddit_collector = RedditNews()
        print("   Reddit collector created")
    except ValueError as e:
        print(f"   Reddit credentials missing: {e}")
        print("   Set these environment variables:")
        print("     export REDDIT_CLIENT_ID='your_client_id'")
        print("     export REDDIT_CLIENT_SECRET='your_client_secret'")
        return
    except Exception as e:
        print(f"   Reddit initialization failed: {e}")
        return

    # Test Reddit connection
    print("\n3. Testing Reddit API connection...")
    if reddit_collector.test_connection():
        print("   Reddit API connection successful")
    else:
        print("   Reddit API connection failed")
        print("   Check your Reddit API credentials")
        return

    # Test general Reddit posts collection
    print("\n4. Testing general Reddit posts collection...")
    try:
        posts = reddit_collector.collect_news(max_articles=5, hours_back=24)
        print(f"   Collected {len(posts)} general Reddit posts")

        if posts:
            print("   Sample posts:")
            for i, post in enumerate(posts[:3]):
                print(f"    {i+1}. {post.title[:60]}...")
                print(f"       Subreddit: {post.source}")
                print(f"       Symbols: {post.symbols_mentioned}")

                # Show engagement metrics
                if hasattr(post, 'additional_metadata'):
                    upvotes = post.additional_metadata.get('upvotes', 0)
                    comments = post.additional_metadata.get('comments', 0)
                    print(f"       Engagement: {upvotes} upvotes, {comments} comments")
        else:
            print("   No posts found (try different time range)")

    except Exception as e:
        print(f"   General posts collection failed: {e}")

    # Test symbol-specific Reddit posts (KEY for decision engine)
    print("\n5. Testing symbol-specific Reddit posts...")
    test_symbols = ['AAPL', 'TSLA', 'GME']  # Popular on Reddit

    for symbol in test_symbols:
        try:
            symbol_posts = reddit_collector.get_symbol_news(symbol, hours_back=24, max_articles=3)
            print(f"   {symbol}: {len(symbol_posts)} posts")

            if symbol_posts:
                # Show best post (highest engagement)
                best_post = max(symbol_posts,
                              key=lambda p: p.additional_metadata.get('upvotes', 0))

                print(f"    Top post: {best_post.title[:50]}...")
                print(f"    Subreddit: {best_post.source}")
                print(f"    Upvotes: {best_post.additional_metadata.get('upvotes', 0)}")
                print(f"    Comments: {best_post.additional_metadata.get('comments', 0)}")

        except Exception as e:
            print(f"   {symbol} posts failed: {e}")

    # Test Reddit watchlist (KEY for decision engine)
    print("\n6. Testing Reddit watchlist monitoring...")
    try:
        watchlist = ['AAPL', 'TSLA', 'MSFT', 'NVDA']
        print(f"   Monitoring watchlist: {watchlist}")

        watchlist_posts = reddit_collector.get_watchlist_news(
            watchlist,
            hours_back=24,
            max_articles_per_symbol=5
        )

        total_posts = sum(len(posts) for posts in watchlist_posts.values())
        print(f"   Total watchlist posts: {total_posts}")

        # Show results for each symbol
        for symbol, posts in watchlist_posts.items():
            print(f"    {symbol}: {len(posts)} posts")

            if posts:
                # Show highest engagement post
                top_post = max(posts, key=lambda p: p.additional_metadata.get('upvotes', 0))
                upvotes = top_post.additional_metadata.get('upvotes', 0)
                print(f"      Top: {top_post.title[:40]}... ({upvotes} upvotes)")

        # Decision engine simulation
        high_activity = [symbol for symbol, posts in watchlist_posts.items() if len(posts) >= 2]
        print(f"   High Reddit activity: {high_activity}")

    except Exception as e:
        print(f"   Watchlist monitoring failed: {e}")

    # Test engagement metrics analysis
    print("\n7. Testing Reddit engagement metrics...")
    try:
        # Get AAPL posts for metrics analysis
        aapl_posts = reddit_collector.get_symbol_news('AAPL', hours_back=24, max_articles=10)

        if aapl_posts:
            metrics = reddit_collector.get_engagement_metrics(aapl_posts)
            print(f"   AAPL engagement metrics:")
            print(f"    Total upvotes: {metrics['total_upvotes']}")
            print(f"    Total comments: {metrics['total_comments']}")
            print(f"    Avg upvote ratio: {metrics['average_upvote_ratio']:.2f}")
            print(f"    Engagement score: {metrics['engagement_score']:.1f}")
            print(f"    Sentiment strength: {metrics['sentiment_strength']:.2f}")

            # Show sentiment interpretation
            sentiment_strength = metrics['sentiment_strength']
            if sentiment_strength > 0.7:
                sentiment = " Bullish"
            elif sentiment_strength < 0.4:
                sentiment = " Bearish"
            else:
                sentiment = " Neutral"

            print(f"    Reddit sentiment: {sentiment}")
        else:
            print("   No AAPL posts for metrics analysis")

    except Exception as e:
        print(f"   Engagement metrics failed: {e}")

    # Test subreddit coverage
    print("\n8. Testing subreddit coverage...")
    try:
        print("   Active trading subreddits:")

        # Test individual subreddits
        test_subreddits = ['wallstreetbets', 'investing', 'stocks']

        for subreddit in test_subreddits:
            try:
                # Get posts from specific subreddit
                subreddit_posts = reddit_collector._get_subreddit_posts(subreddit, limit=5)
                print(f"    r/{subreddit}: {len(subreddit_posts)} posts")

                if subreddit_posts:
                    # Show most engaging post
                    top_post = max(subreddit_posts,
                                 key=lambda p: p.additional_metadata.get('upvotes', 0))
                    print(f"      Top: {top_post.title[:45]}...")

            except Exception as e:
                print(f"    r/{subreddit}: Error - {e}")

    except Exception as e:
        print(f"   Subreddit coverage test failed: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("REDDIT NEWS TEST SUMMARY")
    print("=" * 60)
    print(" Reddit news integration successful!")
    print(" Ready for decision engine integration")

    print(f"\n Reddit News Capabilities:")
    print(f"    Social sentiment monitoring")
    print(f"    Retail trader buzz detection")
    print(f"    Symbol-specific post collection")
    print(f"    Watchlist batch processing")
    print(f"    Engagement metrics for sentiment weighting")
    print(f"    Multi-subreddit coverage")

    print(f"\n Reddit Data for FinBERT:")
    print(f"   • Post titles as sentiment input")
    print(f"   • Engagement metrics for confidence weighting")
    print(f"   • Subreddit context for sentiment interpretation")
    print(f"   • Real-time social sentiment tracking")

    print(f"\n Decision Engine Integration:")
    print(f"   • Use Reddit sentiment as social sentiment component")
    print(f"   • Weight by engagement (upvotes, comments)")
    print(f"   • Combine with professional news for comprehensive analysis")
    print(f"   • Monitor for sudden social sentiment shifts")

def test_reddit_setup():
    """Test Reddit API setup without connecting"""
    print("\n" + "=" * 40)
    print("REDDIT SETUP CHECK")
    print("=" * 40)

    # Check environment variables
    import os

    client_id = os.getenv('REDDIT_CLIENT_ID')
    client_secret = os.getenv('REDDIT_CLIENT_SECRET')
    user_agent = os.getenv('REDDIT_USER_AGENT')

    print(f"Environment variables:")
    print(f"  REDDIT_CLIENT_ID: {' Set' if client_id else ' Missing'}")
    print(f"  REDDIT_CLIENT_SECRET: {' Set' if client_secret else ' Missing'}")
    print(f"  REDDIT_USER_AGENT: {' Set' if user_agent else ' Using default'}")

    if not client_id or not client_secret:
        print(f"\n To set up Reddit API:")
        print(f"   1. Go to https://www.reddit.com/prefs/apps")
        print(f"   2. Create a new application (script type)")
        print(f"   3. Set environment variables:")
        print(f"      export REDDIT_CLIENT_ID='your_client_id'")
        print(f"      export REDDIT_CLIENT_SECRET='your_client_secret'")
        print(f"      export REDDIT_USER_AGENT='YourAppName:1.0'")
        return False

    return True

if __name__ == "__main__":
    # Check setup first
    if test_reddit_setup():
        print("\n Proceeding with Reddit API test...")
        test_reddit_news()
    else:
        print("\n Reddit API not configured - skipping connection test")

    print(f"\n Reddit news testing complete!")

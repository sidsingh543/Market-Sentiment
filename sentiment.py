from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()

def polarity(text: str) -> float:
    if not text:
        return 0.0
    return float(_analyzer.polarity_scores(text)["compound"])

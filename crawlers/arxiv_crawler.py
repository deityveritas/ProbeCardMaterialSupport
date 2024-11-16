import logging
from core.crawler import Crawler
import arxiv
from core.utils import create_session_with_retries

def validate_category(category: str) -> bool:
    valid_categories = [
        "cs", "econ", "q-fin","stat",
        "math", "math-ph", "q-bio", "stat-mech",
        "physics", "astro-ph", "cond-mat", "gr-qc", "hep-ex", "hep-lat", "hep-ph", 
        "hep-th", "nucl-ex", "nucl-th", "physics-ao-ph", "physics-ao-pl", "physics-ao-po",
        "physics-ao-ps", "physics-app-ph", "quant-ph"
    ]
    return category in valid_categories

class ArxivCrawler(Crawler):
    
    def get_citations(self, arxiv_id: str) -> int:
        """
        Retrieves the number of citations for a given paper from Semantic Scholar API based on its arXiv ID.
        """
        base_url = "https://api.semanticscholar.org/v1/paper/arXiv:"
        arxiv_id = arxiv_id.split('v')[0]  # Removes version number, if present

        try:
            response = self.session.get(base_url + arxiv_id)
            response.raise_for_status()  # Raises HTTPError for bad responses
            citations = response.json().get('citations', [])
            return len(citations)
        except Exception as e:
            logging.error(f"Failed to fetch citations for {arxiv_id}: {e}")
            return -1

    def crawl(self):
        """
        Crawls arXiv based on specified parameters in the configuration.
        """
        n_papers = self.cfg.arxiv_crawler.n_papers
        query_terms = self.cfg.arxiv_crawler.query_terms
        year = self.cfg.arxiv_crawler.start_year
        category = self.cfg.arxiv_crawler.arxiv_category

        if not validate_category(category):
            logging.error(f"Invalid arXiv category: {category}")
            exit(1)

        self.session = create_session_with_retries()
        query = f"cat:{category}.* AND " + ' AND '.join([f'all:{q}' for q in query_terms])
        
        if self.cfg.arxiv_crawler.sort_by == 'citations':
            max_results = n_papers * 100
        else:
            max_results = n_papers
        
        search = arxiv.Search(
            query=query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.Relevance if self.cfg.arxiv_crawler.sort_by == 'citations' else arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending
        )
        
        papers = []
        for result in search.results():
            published_date = result.published
            if published_date.year < year:
                continue
            paper_id = result.get_short_id()
            citation_count = self.get_citations(paper_id)
            papers.append({
                'id': result.entry_id,
                'citation_count': citation_count,
                'url': result.pdf_url,
                'title': result.title,
                'authors': [author.name for author in result.authors],
                'abstract': result.summary,
                'published': str(published_date)
            })

        if self.cfg.arxiv_crawler.sort_by == 'citations':
            papers.sort(key=lambda x: x['citation_count'], reverse=True)

        top_n_papers = papers[:n_papers]
        for paper in top_n_papers:
            metadata = {
                'source': 'arXiv',
                'title': paper['title'],
                'abstract': paper['abstract'],
                'url': paper['url'],
                'published': paper['published'],
                'citations': paper['citation_count']
            }
            self.indexer.index_url(paper['url'] + ".pdf", metadata=metadata)

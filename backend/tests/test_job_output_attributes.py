"""
Test output attribute tracking with real JOB (Join Order Benchmark) queries

These tests verify that the fixes for join condition extraction and
output attribute tracking work correctly with real-world benchmark queries.

IMPORTANT: Run these tests in Docker to ensure modules are loaded fresh:
  ./scripts/run_tests.sh
  OR
  docker-compose exec backend pytest tests/test_job_output_attributes.py -v
"""
import pytest
from app.hypergraph.extractor import HypergraphExtractor


class TestJOBOutputAttributes:
    """Test output attributes with JOB-style queries"""

    def test_job_1a_simple_chain(self, imdb_spark):
        """JOB 1a: Simple chain query with MIN aggregates"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mc.note) AS production_note,
               MIN(t.title) AS movie_title,
               MIN(t.production_year) AS movie_year
        FROM company_type AS ct,
             info_type AS it,
             movie_companies AS mc,
             movie_info AS mi,
             title AS t
        WHERE ct.kind = 'production companies'
          AND it.info = 'budget'
          AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
          AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')
          AND ct.id = mc.company_type_id
          AND t.id = mc.movie_id
          AND t.id = mi.movie_id
          AND mc.movie_id = mi.movie_id
          AND it.id = mi.info_type_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Should extract hypergraph successfully
        assert len(hypergraph.edges) > 0, "Should extract tables as edges"
        assert len(hypergraph.nodes) > 0, "Should extract equivalence classes as nodes"

        # Check mc edge has note in output attributes
        mc_edge = next((e for e in hypergraph.edges if e.id == 'mc'), None)
        assert mc_edge is not None, "Should have movie_companies table"

        mc_output_attr_names = [attr.split('.')[-1].split('#')[0] for attr in mc_edge.output_attributes]
        assert 'note' in mc_output_attr_names, "mc.note should be in output (used in SELECT MIN(mc.note))"

        # Check t edge has title and production_year in output attributes
        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert t_edge is not None, "Should have title table"

        t_output_attr_names = [attr.split('.')[-1].split('#')[0] for attr in t_edge.output_attributes]
        assert 'title' in t_output_attr_names, "t.title should be in output (used in SELECT MIN(t.title))"
        assert 'production_year' in t_output_attr_names, "t.production_year should be in output"

    def test_job_2a_star_query(self, imdb_spark):
        """JOB 2a: Star query with multiple tables joining to central table"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title
        FROM company_name AS cn,
             info_type AS it,
             movie_companies AS mc,
             movie_info AS mi,
             title AS t
        WHERE cn.country_code = '[us]'
          AND it.info = 'budget'
          AND t.production_year > 2000
          AND t.id = mi.movie_id
          AND t.id = mc.movie_id
          AND mc.movie_id = mi.movie_id
          AND it.id = mi.info_type_id
          AND cn.id = mc.company_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Verify extraction worked
        assert len(hypergraph.edges) == 5, "Should have 5 tables"

        # Check t.title is tracked as output attribute
        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert t_edge is not None

        t_output_attr_names = [attr.split('.')[-1].split('#')[0] for attr in t_edge.output_attributes]
        assert 'title' in t_output_attr_names, "t.title should be tracked as output attribute"

        # Verify other tables don't have output attributes (not in SELECT)
        mc_edge = next((e for e in hypergraph.edges if e.id == 'mc'), None)
        assert mc_edge is not None
        assert len(mc_edge.output_attributes) == 0, "mc should have no output attributes (not in SELECT)"

    def test_job_with_count_aggregate(self, imdb_spark):
        """Test COUNT(*) and COUNT(DISTINCT) aggregates"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title,
               COUNT(*) AS num_companies,
               COUNT(DISTINCT cn.name) AS company_count
        FROM company_name AS cn,
             movie_companies AS mc,
             title AS t
        WHERE t.production_year > 2000
          AND t.id = mc.movie_id
          AND cn.id = mc.company_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        assert len(hypergraph.edges) == 3

        # t.title should be in output
        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        t_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in t_edge.output_attributes]
        assert 'title' in t_output_attrs

        # cn.name should be in output (used in COUNT(DISTINCT cn.name))
        cn_edge = next((e for e in hypergraph.edges if e.id == 'cn'), None)
        cn_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in cn_edge.output_attributes]
        assert 'name' in cn_output_attrs, "cn.name should be tracked (used in COUNT(DISTINCT))"

    def test_job_with_max_sum_aggregates(self, imdb_spark):
        """Test MAX and SUM aggregates"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MAX(t.production_year) AS latest_year,
               MIN(t.production_year) AS earliest_year,
               COUNT(*) AS movie_count
        FROM title AS t
        JOIN movie_companies AS mc ON t.id = mc.movie_id
        WHERE t.production_year > 2000
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # t.production_year should be tracked (used in MAX and MIN)
        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert t_edge is not None

        t_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in t_edge.output_attributes]
        assert 'production_year' in t_output_attrs, "production_year should be in output (MAX/MIN)"

    def test_job_multiple_aggregates_same_table(self, imdb_spark):
        """Test multiple aggregates from the same table"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mi.info) AS budget,
               MAX(mi.info) AS max_budget,
               COUNT(DISTINCT mi.info) AS info_count
        FROM movie_info AS mi
        JOIN title AS t ON mi.movie_id = t.id
        WHERE t.production_year > 2000
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # mi.info should be tracked once (even though used in MIN, MAX, COUNT)
        mi_edge = next((e for e in hypergraph.edges if e.id == 'mi'), None)
        assert mi_edge is not None

        mi_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in mi_edge.output_attributes]
        assert 'info' in mi_output_attrs, "mi.info should be tracked as output"

    def test_job_with_join_and_filter_attributes(self, imdb_spark):
        """Test that both join attributes and filter attributes are included"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title
        FROM title AS t
        JOIN movie_companies AS mc ON t.id = mc.movie_id
        WHERE t.production_year > 2000
          AND mc.company_id = 1
        """

        hypergraph = extractor.extract_hypergraph(sql)

        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert t_edge is not None

        # Check all attributes are present (join, filter, and output)
        t_all_attrs = [attr.split('.')[-1].split('#')[0] for attr in t_edge.attributes]
        assert 'id' in t_all_attrs, "t.id should be in all attributes (join)"
        assert 'title' in t_all_attrs, "t.title should be in all attributes (output)"
        assert 'production_year' in t_all_attrs, "t.production_year should be in all attributes (filter)"

        # Check output attributes
        t_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in t_edge.output_attributes]
        assert 'title' in t_output_attrs, "t.title should be output attribute"

    def test_job_self_join_with_aggregates(self, imdb_spark):
        """Test self-join with aggregates"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t1.title) AS first_movie,
               MIN(t2.title) AS second_movie
        FROM title AS t1
        JOIN title AS t2 ON t1.production_year = t2.production_year
        WHERE t1.id != t2.id
          AND t1.production_year > 2000
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Both t1 and t2 should have title in output
        t1_edge = next((e for e in hypergraph.edges if e.id == 't1'), None)
        t2_edge = next((e for e in hypergraph.edges if e.id == 't2'), None)

        assert t1_edge is not None
        assert t2_edge is not None

        t1_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in t1_edge.output_attributes]
        t2_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in t2_edge.output_attributes]

        assert 'title' in t1_output_attrs, "t1.title should be in output"
        assert 'title' in t2_output_attrs, "t2.title should be in output"

    def test_job_complex_multi_table_aggregate(self, imdb_spark):
        """Test complex query with aggregates from multiple tables (the user's original query)"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mi.info) AS budget,
               MIN(t.title) AS unsuccessful_movie
        FROM company_name AS cn,
             company_type AS ct,
             info_type AS it1,
             info_type AS it2,
             movie_companies AS mc,
             movie_info AS mi,
             movie_info AS mi_idx,
             title AS t
        WHERE cn.country_code = '[us]'
          AND ct.kind IS NOT NULL
          AND (ct.kind = 'production companies' OR ct.kind = 'distributors')
          AND it1.info = 'budget'
          AND it2.info = 'bottom 10 rank'
          AND t.production_year > 2000
          AND (t.title LIKE 'Birdemic%' OR t.title LIKE '%Movie%')
          AND t.id = mi.movie_id
          AND t.id = mi_idx.movie_id
          AND mi.info_type_id = it1.id
          AND mi_idx.info_type_id = it2.id
          AND t.id = mc.movie_id
          AND ct.id = mc.company_type_id
          AND cn.id = mc.company_id
          AND mc.movie_id = mi.movie_id
          AND mc.movie_id = mi_idx.movie_id
          AND mi.movie_id = mi_idx.movie_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Should successfully extract the complex query
        assert len(hypergraph.edges) == 8, "Should have 8 tables (including 2 info_type and 2 movie_info aliases)"
        assert len(hypergraph.nodes) > 0, "Should have equivalence classes from joins"

        # mi.info should be in output
        mi_edge = next((e for e in hypergraph.edges if e.id == 'mi'), None)
        assert mi_edge is not None, "Should have mi table"
        mi_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in mi_edge.output_attributes]
        assert 'info' in mi_output_attrs, "mi.info should be tracked as output (MIN(mi.info))"

        # t.title should be in output
        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert t_edge is not None, "Should have t table"
        t_output_attrs = [attr.split('.')[-1].split('#')[0] for attr in t_edge.output_attributes]
        assert 'title' in t_output_attrs, "t.title should be tracked as output (MIN(t.title))"

        # Other tables should have no output attributes
        cn_edge = next((e for e in hypergraph.edges if e.id == 'cn'), None)
        assert cn_edge is not None
        assert len(cn_edge.output_attributes) == 0, "cn should have no output attributes"

        # Verify all attributes are present (not just join attributes)
        mi_all_attrs = [attr.split('.')[-1].split('#')[0] for attr in mi_edge.attributes]
        assert 'id' in mi_all_attrs
        assert 'movie_id' in mi_all_attrs
        assert 'info_type_id' in mi_all_attrs
        assert 'info' in mi_all_attrs, "All table attributes should be present, not just join attributes"

    def test_extraction_performance_with_large_query(self, imdb_spark):
        """Test that extraction completes in reasonable time even with complex queries"""
        import time

        extractor = HypergraphExtractor(imdb_spark)

        # Complex query with multiple joins and aggregates
        sql = """
        SELECT MIN(mi.info) AS budget,
               MIN(t.title) AS movie,
               COUNT(*) AS count,
               COUNT(DISTINCT cn.name) AS companies
        FROM company_name AS cn,
             company_type AS ct,
             movie_companies AS mc,
             movie_info AS mi,
             title AS t
        WHERE cn.country_code = '[us]'
          AND ct.kind = 'production companies'
          AND t.production_year > 2000
          AND t.id = mi.movie_id
          AND t.id = mc.movie_id
          AND mc.movie_id = mi.movie_id
          AND ct.id = mc.company_type_id
          AND cn.id = mc.company_id
        """

        start_time = time.time()
        hypergraph = extractor.extract_hypergraph(sql)
        elapsed_time = time.time() - start_time

        assert elapsed_time < 5.0, f"Extraction should complete quickly (took {elapsed_time:.2f}s)"
        assert len(hypergraph.edges) == 5
        assert len(hypergraph.nodes) > 0

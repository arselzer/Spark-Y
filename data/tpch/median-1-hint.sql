select
    /*+ FK(ps_partkey, p_partkey), FK(n_regionkey, r_regionkey), FK(ps_suppkey, s_suppkey), FK(s_nationkey, n_nationkey), PK(ps_partkey, ps_suppkey) */
        median(s_acctbal)
		from
            part,
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
            AND p_retailprice >
                (SELECT avg (p_retailprice) FROM part)
            and r_name IN ('EUROPE', 'ASIA')
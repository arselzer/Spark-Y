select
/*+ FK(ps_partkey, p_partkey), FK(n_regionkey, r_regionkey), FK(ps_suppkey, s_suppkey), FK(s_nationkey, n_nationkey) */
			median(ps_supplycost)
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
            and p_retailprice < (select mean(p2.p_retailprice) from part p2)
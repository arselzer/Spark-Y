		select
			count(*)
		from
			customer  join orders on
				c_custkey = o_custkey
				and o_comment not like '%:1%:2%'
		group by
			c_custkey
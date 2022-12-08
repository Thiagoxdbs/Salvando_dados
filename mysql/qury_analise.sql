SELECT 	cc.id,cc.empresa,cc.meta_num,ro.active_accounts AS emails_ativos,
		ro.running_campaigns AS campanhas_ativas_abril_2022,cc.dominio_nosso,

		#res_ago_21.res_ago_2021,
		ago_21.int_ago_2021,
		#res_set_21.res_set_2021,
		set_21.int_set_2021,
		#res_out_21.res_out_2021,
		out_21.int_out_2021,
		#res_nov_21.res_nov_2021,
		nov_21.int_nov_2021,
		#res_dez_21.res_dez_2021,
		dez_21.int_dez_2021,
		#res_jan_22.res_jan_2022,
		jan_22.int_jan_2022,
		#res_fev_22.res_fev_2022,
		fev_22.int_fev_2022,
		#res_mar_22.res_mar_2022,
		mar_22.int_mar_2022,
		#res_abr_22.res_abr_2022,
		abr_22.int_abr_2022

FROM cadastro_clientes AS cc

	/*LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_jan_2022 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-01'
				GROUP BY cliente_id_fk) AS res_jan_22 ON res_jan_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_fev_2022 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-02'
				GROUP BY cliente_id_fk) AS res_fev_22 ON res_fev_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_mar_2022 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-03'
				GROUP BY cliente_id_fk) AS res_mar_22 ON res_mar_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_abr_2022 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-04'
				GROUP BY cliente_id_fk) AS res_abr_22 ON res_abr_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_ago_2021 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-08'
				GROUP BY cliente_id_fk) AS res_ago_21 ON res_ago_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_set_2021 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-09'
				GROUP BY cliente_id_fk) AS res_set_21 ON res_set_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_out_2021 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-10'
				GROUP BY cliente_id_fk) AS res_out_21 ON res_out_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_nov_2021 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-11'
				GROUP BY cliente_id_fk) AS res_nov_21 ON res_nov_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS res_dez_2021 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-12'
				GROUP BY cliente_id_fk) AS res_dez_21 ON res_dez_21.id = cc.id*/



	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_jan_2022 FROM sales_base_leads
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-01' AND
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS jan_22 ON jan_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_fev_2022 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-02' AND 
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS fev_22 ON fev_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_mar_2022 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-03' AND 
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS mar_22 ON mar_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_abr_2022 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2022-04' AND 
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS abr_22 ON abr_22.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_ago_2021 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-08' AND 
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS ago_21 ON ago_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_set_2021 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-09' AND 
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS set_21 ON set_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_out_2021 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-10' AND 
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS out_21 ON out_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_nov_2021 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-11' AND 
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS nov_21 ON nov_21.id = cc.id

	LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS int_dez_2021 FROM sales_base_leads 
				WHERE DATE_FORMAT(ultima_resposta,'%Y-%m') = '2021-12' AND 	
				status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS dez_21 ON dez_21.id = cc.id

	LEFT JOIN robo_python_hoje AS ro ON ro.id = cc.id
	
	ORDER BY cc.id ASC

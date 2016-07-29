package br.com.serverAPM.persistence;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import waba.sys.Vm;

public class DownSincBusiness extends BBBusiness {

	
	public String[] getResumoAnalitico(int cdUnidade, int cdRota, int dtPrevenda){
		String[] retorno =null; 
		try {
			Connection connection = beginTransaction();
			
			
			Statement stm = connection.createStatement();
			//stm.setQueryTimeout(10);
			  
			   ResultSet rs = stm.executeQuery("select "+
					" sum(case when  substring(d_pedido,96,1)not in ('C')  then 1 else 0 end)  qtdNormais, "+
					" sum(case when  substring(d_pedido,96,1)='C' and substring(d_pedido,110,1) not in('L','S')  then 1 else 0 end)  qtdCancelados, "+
					" sum(case when  substring(d_pedido,96,1)='C' and substring(d_pedido,110,1) ='L'  then 1 else 0 end)  qtdCanceladosDilatacao, "+
					" sum(case when  substring(d_pedido,96,1)='C' and substring(d_pedido,110,1) ='S'  then 1 else 0 end)  qtdCanceladosPrazo, "+
					" sum(1)  qtdTotal "+
					" from nsf_pedidos.dbo.pedidos "+
					" where unidade ="+cdUnidade+" and rota = "+cdRota+" and data = "+dtPrevenda+" and left(d_pedido,1)='1' ");

			   
			   while (rs.next()) {
				     retorno = new String[5];				
				     System.out.println( " Unidade = "+cdUnidade+ " Rota="+cdRota+" Total Pedidos=" + rs.getString(5));
				     retorno[0]=rs.getString(1);
				     retorno[1]=rs.getString(2);
				     retorno[2]=rs.getString(3);
				     retorno[3]=rs.getString(4);
				     retorno[4]=rs.getString(5);
			   }
			   rs.close();
			   stm.close();
			   closeConnection(connection);
			   
		} catch (Exception e) {			
			e.printStackTrace();
		}
		
		
		return retorno;
	}
	
	

	public String[] getOcupacao(int cdAgente, String tpOperacao)throws Exception{
		String[]   retorno=null; 
		int contador=0;		
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  
			   ResultSet rs = stm.executeQuery("select   ocu.cd_ocupacao, "+
												"	ocu.ds_ocupacao, "+					
												"	case log.tp_operacao "+
												"		 when  'I' then 1 "+
												"		 when  'U' then 2 "+
												"		 when  'D' then 3 "+
												"		 when  'A' then 4 "+
												"	end as STATUS, "+
												"	log.id_key, "+
												"	log.CD_SINC_USUARIO "+
												" from ocupacao ocu right join log_alteracao_usuario log  "+
												" on log.id_key = 'CD_OCUPACAO=' || CAST(ocu.cd_ocupacao AS VARCHAR(10))  "+
												" where log.cd_usuario = "+cdAgente+
												" and   log.nm_tabela = 'OCUPACAO' "+
												" and   log.dt_sincronismo is null "+
												" and   log.tp_operacao = '"+tpOperacao+"' " +
											    " order by log.cd_sinc_usuario");

			 String[] retornoTemp = new String[1024];
			 StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
					   regTemp.append(rs.getString(1)).append(delimiter).append(rs.getString(2)).append(delimiter).append(rs.getString(3));
					   regTemp.append(delimiter).append(rs.getString(4)).append(delimiter).append(rs.getString(5));				
					   System.out.println(regTemp);
					   retornoTemp[contador]=regTemp.toString();
					   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);	
			   rs.close();
			   stm.close();
			  
			   
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}
		
		
		return retorno;
	}
	public String[] getFichaCadFam(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  
			   ResultSet rs = stm.executeQuery(" select  fam.cd_segmento, "+
					   						    " fam.cd_area, "+
					   						    " fam.cd_microarea, "+
					   						    " fam.nr_familia, "+
											    " fam.cd_ano, "+
												" COALESCE(fam.ds_endereco,'') as ds_endereco,"+
												" fam.num_endereco,"+
												" COALESCE(fam.ds_bairro,'') as ds_bairro,"+
												" COALESCE(fam.ds_cep,'') as ds_cep,"+
												" fam.cd_municipio,"+
												" COALESCE(fam.dt_cadastro,'') as dt_cadastro,"+
												" COALESCE(fam.cd_uf,'') as cd_uf,"+
												" fam.cd_tpcasa,"+
												" COALESCE(fam.ds_tpcasaesp,'') as ds_tpcasaesp,"+
												" fam.num_comodos,"+
												"  COALESCE(fam.fl_energia,'') as fl_energia,"+
												" fam.cd_destlixo,"+
												" fam.cd_trataagua,"+
												" fam.cd_abasteagua,"+
												" fam.cd_fezesurina,"+
												" fam.cd_doencaprocu,"+
												"  COALESCE(fam.ds_doencaprocuesp,'') as ds_doencaprocuesp,"+
												" fam.cd_grupocomu,"+
												"  COALESCE(fam.ds_grupocomuesp,'') as ds_grupocomuesp,"+
												" fam.cd_meioscomuni,"+
												"  COALESCE(fam.ds_meioscomuniesp,'') as ds_meioscomuniesp,"+
												" fam.cd_meiostrans,"+
												"  COALESCE(fam.ds_meiostransesp,'') as ds_meiostransesp,"+
												"  COALESCE(fam.fl_possuiplan,'') as fl_possuiplan,"+	
												" fam.num_pesplan,"+
												"  COALESCE(fam.nm_plano,'') as nm_plano,"+
												"  COALESCE(fam.ds_observacao,'') as ds_observacao,"+	
												" case log.tp_operacao "+
												"	 when  'I' then 1 "+
												"	 when  'U' then 2 "+
												"	 when  'D' then 3 "+
												"	 when  'A' then 4 "+
												" end as STATUS, "+
												" log.id_key, "+
												" log.CD_SINC_USUARIO "+
										" from fichacadfam fam right join log_alteracao_usuario log  "+
										" on log.id_key = 'CD_SEGMENTO=' || CAST(fam.CD_SEGMENTO AS VARCHAR(10)) "+
										"			       ||';CD_AREA=' || CAST(fam.CD_AREA AS VARCHAR(10)) "+
										"			       ||';CD_MICROAREA=' || CAST(fam.CD_MICROAREA AS VARCHAR(10)) "+
										"			       ||';NR_FAMILIA=' || CAST(fam.NR_FAMILIA AS VARCHAR(10)) "+
										"			       || ';CD_ANO=' || CAST(fam.CD_ANO AS VARCHAR(4)) "+ 
										" where log.cd_usuario = "+cdAgente+
										" and   log.nm_tabela = 'FICHACADFAM' "+
										" and   log.dt_sincronismo is null "+
										" and   log.tp_operacao = '"+tpOperacao+"'"+
										" order by log.cd_sinc_usuario ");

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<34 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
			   
		}catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	
	public String[] getPacFamilia(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  fam.cd_segmento, "+
					    	" fam.cd_area, "+
						    " fam.cd_microarea, "+
						    " fam.nr_familia, "+
							" fam.cd_paciente, "+
							" fam.cd_ano, "+	
							" COALESCE(fam.nm_paciente,'') as nm_paciente, "+
							" COALESCE(fam.dt_nascimento,'') as dt_nascimento, "+
							" fam.num_idade, "+
							" COALESCE(fam.fl_sexo,'') as fl_sexo, "+
							" COALESCE(fam.fl_menorquinze,'') as fl_menorquinze, "+
							" COALESCE(fam.fl_alfabetizado,'') as fl_alfabetizado, "+
							" COALESCE(fam.fl_freqescola,'') as fl_freqescola, "+
							" fam.cd_ocupacao, "+
							" COALESCE(fam.fl_alc,'') as fl_alc, "+
							" COALESCE(fam.fl_cha,'') as fl_cha, "+
							" COALESCE(fam.fl_def,'') as fl_def, "+
							" COALESCE(fam.fl_dia,'') as fl_dia, "+
							" COALESCE(fam.fl_dme,'') as fl_dme, "+
							" COALESCE(fam.fl_epi,'') as fl_epi, "+
							" COALESCE(fam.fl_ges,'') as fl_ges, "+
							" COALESCE(fam.fl_han,'') as fl_han, "+
							" COALESCE(fam.fl_ha,'') as fl_ha, "+
							" COALESCE(fam.fl_mal,'') as fl_mal, "+
							" COALESCE(fam.fl_tbc,'') as fl_tbc, "+	
							" COALESCE(fam.fl_maior,'') as fl_maior, "+
							" case log.tp_operacao "+
	   					"	 when  'I' then 1 "+
	   				    "	 when  'U' then 2 "+
	   					"	 when  'D' then 3 "+
	   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from pacfamilia fam right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(fam.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       ||';CD_AREA=' || CAST(fam.CD_AREA AS VARCHAR(10)) "+
					"			       ||';CD_MICROAREA=' || CAST(fam.CD_MICROAREA AS VARCHAR(10)) "+
					"			       ||';NR_FAMILIA=' || CAST(fam.NR_FAMILIA AS VARCHAR(10)) "+
					"                  ||';CD_PACIENTE=' || CAST(fam.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(fam.CD_ANO AS VARCHAR(4)) "+ 
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'PACFAMILIA' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<28 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
			   
			   
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	public String[] getFichaTb(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  tb.cd_segmento, "+
					    	" tb.cd_area, "+
						    " tb.cd_microarea, "+
						    " tb.nr_familia, "+
							" tb.cd_paciente, "+
							" tb.cd_ano, "+	
							" tb.num_comunicantes, "+
							" tb.num_comunicantes5, "+
							" COALESCE(tb.ds_observacao,'') as ds_observacao, "+
							" case log.tp_operacao "+
	   					"	 when  'I' then 1 "+
	   				    "	 when  'U' then 2 "+
	   					"	 when  'D' then 3 "+
	   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichatb tb right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(tb.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       ||';CD_AREA=' || CAST(tb.CD_AREA AS VARCHAR(10)) "+
					"			       ||';CD_MICROAREA=' || CAST(tb.CD_MICROAREA AS VARCHAR(10)) "+
					"			       ||';NR_FAMILIA=' || CAST(tb.NR_FAMILIA AS VARCHAR(10)) "+
					"                  ||';CD_PACIENTE=' || CAST(tb.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(tb.CD_ANO AS VARCHAR(4)) "+ 
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHATB' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<11 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		}catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	public String[] getFichaTbAcomp(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  tb.cd_segmento, "+
					    	" tb.cd_area, "+
						    " tb.cd_microarea, "+
						    " tb.nr_familia, "+
							" tb.cd_paciente, "+							
							" tb.cd_ano,	 "+
							" tb.cd_mes, "+	
							" tb.dt_visita, "+
							" tb.fl_medicacao, "+
							" tb.fl_reaindese, "+
							" tb.dt_consulta, "+
							" tb.fl_escarro, "+
							" tb.num_examinados, "+
							" tb.num_combcg,	 "+
							" case log.tp_operacao "+
	   					"	 when  'I' then 1 "+
	   				    "	 when  'U' then 2 "+
	   					"	 when  'D' then 3 "+
	   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichatbacomp tb right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(tb.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       ||';CD_AREA=' || CAST(tb.CD_AREA AS VARCHAR(10)) "+
					"			       ||';CD_MICROAREA=' || CAST(tb.CD_MICROAREA AS VARCHAR(10)) "+
					"			       ||';NR_FAMILIA=' || CAST(tb.NR_FAMILIA AS VARCHAR(10)) "+
					"                  ||';CD_PACIENTE=' || CAST(tb.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(tb.CD_ANO AS VARCHAR(4)) "+ 
					"				   || ';CD_MES=' || CAST(tb.cd_mes AS VARCHAR(2)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHATBACOMP' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<16 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();			   
			   
		}catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	public String[] getFichaHan(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  COALESCE(han.cd_segmento,0) as cd_segmento, "+
					    	" COALESCE(han.cd_area,0) as cd_area, "+
						    " COALESCE(han.cd_microarea,0) as cd_microarea, "+
						    " COALESCE( han.nr_familia,0) as nr_familia, "+
							" COALESCE(han.cd_paciente,0) as cd_paciente, "+							
							" COALESCE(han.cd_ano,0) as cd_ano,	 "+
							" COALESCE(han.num_comunicantes,0) as num_comunicantes, "+
							" han.ds_observacao, "+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichahan han right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(han.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(han.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(han.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(han.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(han.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(han.CD_ANO AS VARCHAR(4)) "+				
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAHAN' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<10 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	public String[] getFichaHanAcomp(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  han.cd_segmento, "+
					    	" han.cd_area, "+
						    " han.cd_microarea, "+
						    " han.nr_familia, "+
							" han.cd_paciente, "+							
							" han.cd_ano,	 "+
							" han.cd_mes, "+
							" han.dt_visita, "+
							" han.fl_medicacao, "+
							" han.dt_ultdose, "+
							" han.fl_autocuidados, "+
							" han.dt_consulta, "+
							" han.num_examinados, "+
							" han.num_combcg, "+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichahanacomp han right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(han.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(han.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(han.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(han.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(han.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(han.CD_ANO AS VARCHAR(4)) "+	
					" 				   || ';CD_MES=' || CAST(han.cd_mes AS VARCHAR(2)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAHANACOMP' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<16 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();			   
		}catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}			
		return retorno;
	}
	
	public String[] getFichaHa(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  ha.cd_segmento, "+
					    	" ha.cd_area, "+
						    " ha.cd_microarea, "+
						    " ha.nr_familia, "+
							" ha.cd_paciente, "+							
							" ha.cd_ano,	 "+
							" ha.fl_fumante, "+
							" ha.ds_observacao, "+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichaha ha right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(ha.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(ha.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(ha.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(ha.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(ha.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(ha.CD_ANO AS VARCHAR(4)) "+				
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAHA' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<10 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		}catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	public String[] getFichaHaAcomp(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  ha.cd_segmento, "+
					    	" ha.cd_area, "+
						    " ha.cd_microarea, "+
						    " ha.nr_familia, "+
							" ha.cd_paciente, "+							
							" ha.cd_ano,	 "+
							" ha.cd_mes, "+
							" ha.dt_visita, "+
							" ha.fl_dieta, "+
							" ha.fl_medicacao, "+
							" ha.fl_exercicio, "+
							" ha.ds_pressao, "+
							" ha.dt_ultconsulta, "+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichahaacomp ha right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(ha.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(ha.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(ha.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(ha.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(ha.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(ha.CD_ANO AS VARCHAR(4)) "+	
					" 				   || ';CD_MES=' || CAST(ha.cd_mes AS VARCHAR(2)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAHAACOMP' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<15 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}			
		return retorno;
	}
	
	
	public String[] getFichaDia(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  COALESCE(dia.cd_segmento,0) as cd_segmento, "+
					    	" COALESCE(dia.cd_area,0) as cd_segmento, "+
						    " COALESCE(dia.cd_microarea,0) as cd_segmento, "+
						    " COALESCE(dia.nr_familia,0) as cd_segmento, "+
							" COALESCE(dia.cd_paciente,0) as cd_segmento, "+							
							" COALESCE(dia.cd_ano,0) as cd_segmento, "+
							" dia.ds_observacao, "+							
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichadia dia right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(dia.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(dia.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(dia.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(dia.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(dia.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(dia.CD_ANO AS VARCHAR(4)) "+				
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHADIA' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<9 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}			
		return retorno;
	}
	
	
	public String[] getFichaDiaAcomp(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  dia.cd_segmento, "+
					    	" dia.cd_area, "+
						    " dia.cd_microarea, "+
						    " dia.nr_familia, "+
							" dia.cd_paciente, "+							
							" dia.cd_ano,	 "+
							" dia.cd_mes, "+
							" dia.dt_visita, "+
							" dia.fl_dieta, "+
							" dia.fl_exercicio, "+
							" dia.fl_insulina, "+
							" dia.fl_hipoglicemiante, "+
							" dia.dt_ultconsulta, "+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichadiaacomp dia right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(dia.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(dia.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(dia.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(dia.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(dia.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(dia.CD_ANO AS VARCHAR(4)) "+	
					" 				   || ';CD_MES=' || CAST(dia.cd_mes AS VARCHAR(2)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHADIAACOMP' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<15 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}	
		return retorno;
	}
	
	public String[] getFichaGes(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  ges.cd_segmento, "+
					    	" ges.cd_area, "+
						    " ges.cd_microarea, "+
						    " ges.nr_familia, "+
							" ges.cd_paciente, "+							
							" ges.cd_ano,	 "+
							" ges.dt_ultregra, "+
							" ges.dt_parto, "+
							" ges.dt_vacina1, "+
							" ges.dt_vacina2, "+
							" ges.dt_vacina3, "+
							" ges.dt_vacinar, "+
							" ges.fl_seisgesta, "+
							" ges.fl_natimorto, "+
							" ges.fl_36anosmais, "+
							" ges.fl_menos20, "+
							" ges.fl_sangramento , "+
							" ges.fl_edema, "+
							" ges.fl_diabetes, "+
							" ges.fl_pressaoalta, "+
							" ges.dt_nascvivo, "+
							" ges.dt_nascmorto, "+
							" ges.dt_aborto, "+
							" ges.dt_puerperio1, "+
							" ges.dt_puerperio2, "+
							" ges.ds_observacao, "+						
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichages ges right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(ges.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(ges.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(ges.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(ges.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(ges.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(ges.CD_ANO AS VARCHAR(4)) "+				
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAGES' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<28 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		}  catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	
	public String[] getFichaGesAcomp(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  ges.cd_segmento, "+
					    	" ges.cd_area, "+
						    " ges.cd_microarea, "+
						    " ges.nr_familia, "+
							" ges.cd_paciente, "+							
							" ges.cd_ano,	 "+
							" ges.cd_mes, "+
							" ges.fl_estadonutri, "+
							" ges.dt_consprenatal, "+
							" ges.dt_visita, "+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichagesacomp ges right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(ges.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(ges.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(ges.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(ges.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(ges.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(ges.CD_ANO AS VARCHAR(4)) "+	
					" 				   || ';CD_MES=' || CAST(ges.cd_mes AS VARCHAR(2)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAGESACOMP' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<12 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	public String[] getFichaCri(int cdAgente, String tpOperacao)throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection = null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  cri.cd_segmento, "+
					    	" cri.cd_area, "+
						    " cri.cd_microarea, "+
						    " cri.nr_familia, "+
							" cri.cd_paciente, "+							
							" cri.cd_ano,	 "+
							" cri.CD_AVALIACAOALIMEN, "+
							" cri.DS_TIPOALIMENTACAO, "+
							" cri.FL_IMUNIZADO, "+
							" cri.FL_COMPLETARESQ, "+
							" cri.FL_DIARREIA, "+
							" cri.FL_USARAMTRO, "+
							" cri.FL_INTERNAMENTOS, "+
							" cri.FL_IRA, "+
							" cri.FL_CAXUMBA, "+
							" cri.FL_CATAPORA, "+
							" cri.FL_BAIXOPESONASC, "+
							" cri.FL_DEFICIENCIAS,"+						
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichacrianca cri right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(cri.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(cri.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(cri.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(cri.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(cri.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(cri.CD_ANO AS VARCHAR(4)) "+				
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHACRIANCA' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<20 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}			
		return retorno;
	}
	
	
	public String[] getFichaCriAcomp(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  cri.cd_segmento, "+
					    	" cri.cd_area, "+
						    " cri.cd_microarea, "+
						    " cri.nr_familia, "+
							" cri.cd_paciente, "+							
							" cri.cd_ano,	 "+
							" cri.cd_mes, "+
							" cri.dt_quadronutri, "+
							" cri.num_peso, "+
							" cri.num_altura,	"+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichacriancaacomp cri right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(cri.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(cri.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(cri.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(cri.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(cri.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(cri.CD_ANO AS VARCHAR(4)) "+	
					" 				   || ';CD_MES=' || CAST(cri.cd_mes AS VARCHAR(2)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHACRIANCAACOMP' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<12 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}			
		return retorno;
	}
	
	public String[] getFichaIdoso(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection = null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  ido.cd_segmento, "+
					    	" ido.cd_area, "+
						    " ido.cd_microarea, "+
						    " ido.nr_familia, "+
							" ido.cd_paciente, "+							
							" ido.cd_ano,	 "+
							" ido.fl_fumante, "+
							" ido.dt_dtp, "+
							" ido.dt_influenza, "+
							" ido.dt_pneumonococos, "+
							" ido.fl_hipertensao, "+
							" ido.fl_tuberculose, "+
							" ido.fl_alzheimer, "+
							" ido.fl_deffisica, "+
							" ido.fl_hanseniase, "+
							" ido.fl_malparkson, "+
							" ido.fl_cancer, "+
							" ido.fl_acamado, "+
							" ido.fl_internamentos, "+
							" ido.fl_sequelasavc, "+
							" ido.fl_alcolatra, "+						
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichaidoso ido right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(ido.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(ido.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(ido.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(ido.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(ido.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(ido.CD_ANO AS VARCHAR(4)) "+				
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAIDOSO' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<23 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	
	public String[] getFichaIdosoAcomp(int cdAgente, String tpOperacao) throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  ido.cd_segmento, "+
					    	" ido.cd_area, "+
						    " ido.cd_microarea, "+
						    " ido.nr_familia, "+
							" ido.cd_paciente, "+							
							" ido.cd_ano,	 "+
							" ido.cd_mes, "+
							" ido.fl_estadonutri, "+
							" ido.dt_visita, "+
							" ido.dt_acomppsf,	"+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from fichaidosoacomp ido  right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_SEGMENTO=' || CAST(ido.CD_SEGMENTO AS VARCHAR(10)) "+
					"			       || ';CD_AREA=' || CAST(ido.CD_AREA AS VARCHAR(10)) "+
					"			       || ';CD_MICROAREA=' || CAST(ido.CD_MICROAREA AS VARCHAR(10)) "+
					"			       || ';NR_FAMILIA=' || CAST(ido.NR_FAMILIA AS VARCHAR(10)) "+
					"                  || ';CD_PACIENTE=' || CAST(ido.CD_PACIENTE AS VARCHAR(10)) "+ 
					"			       || ';CD_ANO=' || CAST(ido.CD_ANO AS VARCHAR(4)) "+	
					" 				   || ';CD_MES=' || CAST(ido.cd_mes AS VARCHAR(2)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'FICHAIDOSOACOMP' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<12 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}			
		return retorno;
	}
	
	public String[] getAgente(int cdAgente, String tpOperacao)throws Exception{
		String[] retorno =null; 
		String[] retornoTemp=null;
		int contador=0;
		Connection connection = null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  a.cd_agente," +
			  				" a.nm_agente, "+
			  				" a.ds_login,"+
			  				" a.ds_senha, "+			  				
			  				" e.cod_seg, "+
					    	" e.cod_area, "+
						    " e.cod_microa, "+
						    " date_part('year',  CURRENT_TIMESTAMP) as  cd_ano,"+
							" case log.tp_operacao "+
		   					"	 when  'I' then 1 "+
		   				    "	 when  'U' then 2 "+
		   					"	 when  'D' then 3 "+
		   		            "	 when  'A' then 4 "+
							" end as status, "+
							" log.id_key, "+
							" log.cd_sinc_usuario "+
				        " from agente a inner join equipe e on a.cd_agente=cast(E.cod_prof as int) and E.cod_atp='77' " +
				        " right join log_alteracao_usuario log "+										
					" on log.id_key = 'CD_AGENTE=' || CAST(a.CD_AGENTE AS VARCHAR(10)) "+
					" where log.cd_usuario = "+cdAgente+
					" and   log.cd_sistema_ext='PALM' "+
					" and   log.nm_tabela = 'AGENTE' "+
					" and   log.dt_sincronismo is null "+
					" and   log.tp_operacao = '"+tpOperacao+"'"+
					" order by log.cd_sinc_usuario ";
			   ResultSet rs = stm.executeQuery(sql);

			   retornoTemp = new String[1004];			  
			   StringBuffer regTemp = new StringBuffer(1024);
			   while (rs.next()) {
				   regTemp.setLength(0);
				   regTemp.append(rs.getString(1));				  
				   for(int i =0;i<10 ;i++){
					   regTemp.append(delimiter).append(rs.getString(i+2));					  
				   }				   				
				   System.out.println(regTemp);
				   retornoTemp[contador]=regTemp.toString();
				   contador++;
			   }
			   retorno = new String[contador];
			   Vm.copyArray(retornoTemp,0,retorno,0,contador);			  
			   rs.close();
			   stm.close();
		} catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}	
		return retorno;
	}
	
	public String[] validateAgente(int cdAgente) throws Exception{
		String[] retorno =null; 	
		Connection connection = null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			  String sql=" select  a.cd_agente, "+
						 "	a.nm_agente,  "+
						 "	a.ds_login, "+
						 "	a.ds_senha,  "+			  				
						 "	e.cod_seg as cd_segmento,  "+
						 "	e.cod_area as cd_area,  "+
						 "	e.cod_microa as cd_microarea "+
						 " from agente a inner join equipe e on a.cd_agente=cast(E.cod_prof as int) and E.cod_atp='77'  "+
						 " where cd_agente ="+cdAgente;
			   ResultSet rs = stm.executeQuery(sql);
			   if (rs.next()) {
				   retorno = new String[7];
				   retorno[0]=rs.getString(1);
				   retorno[1]=rs.getString(2);
				   retorno[2]=rs.getString(3);
				   retorno[3]=rs.getString(4);
				   retorno[4]=rs.getString(5);
				   retorno[5]=rs.getString(6);
				   retorno[6]=rs.getString(7);				   
			   }    
			   rs.close();
			   stm.close();
		}  catch (Exception e) {			
			throw new Exception(e);
		} finally{
			 closeConnection(connection);
		}		
		return retorno;
	}
	
	
	public static void main(String[] args) {
		/*DownSincBusiness  apmBusiness =  new DownSincBusiness();
		apmBusiness.getAgente(16,"I");*/
	}
	
}

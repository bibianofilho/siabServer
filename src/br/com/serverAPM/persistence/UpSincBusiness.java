package br.com.serverAPM.persistence;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import waba.sys.Convert;
import waba.sys.Vm;

public class UpSincBusiness extends BBBusiness {

	
	public void setRegistroSincronizado(int cdAgente, String nmTabela, String tpOperacao,String cdSincUsuario) throws Exception{		
		Connection connection =null;
		try {
			connection = getConnection();			
			Statement stm = connection.createStatement();
			String sql =  " update log_alteracao_usuario  set dt_sincronismo = current_timestamp" +
						  " where cd_sinc_usuario <="+cdSincUsuario+" "+
						  " and cd_sistema_ext='PALM' "+
						  " and tp_operacao = '"+tpOperacao+"'"+
						  " and dt_sincronismo is null"+
						  " and nm_tabela='"+nmTabela+"'"+
						  " and cd_usuario="+cdAgente;
			System.out.println(sql);
			stm.executeUpdate(sql);		
			stm.close();
		} catch (Exception e) {	
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}	
	}
	
	public void deleteFichaCadFam(String[] key) throws Exception{
		
		String[] cols=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			for(int i=0;i<key.length;i++){
				cols=Convert.tokenizeString(key[i],'|');	
				sql.setLength(0);
				sql.append(" delete from fichacadfam  where cd_segmento =").append(cols[0]).append(" and cd_area= ").append(cols[1]);
				sql.append(" and cd_microarea = ").append(cols[2]).append(" and nr_familia =").append(cols[3]).append(" and cd_ano=").append(cols[4]);							  
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}
		
		
	}
	
	public void deleteFicha(String[] key,String nmTabela) throws Exception{
		
		String[] cols=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			for(int i=0;i<key.length;i++){
				cols=Convert.tokenizeString(key[i],'|');	
				sql.setLength(0);
				sql.append(" delete from "+ nmTabela+"  where cd_segmento =").append(cols[0]).append(" and cd_area= ").append(cols[1]);
				sql.append(" and cd_microarea = ").append(cols[2]).append(" and nr_familia =").append(cols[3]).append(" and cd_ano=").append(cols[4]);
				sql.append(" and cd_paciente = ").append(cols[5]);
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			
			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}
		
		
	}
	
	public void deleteFichaAcomp(String[] key,String nmTabela) throws Exception{		
		String[] cols=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			for(int i=0;i<key.length;i++){
				cols=Convert.tokenizeString(key[i],'|');	
				sql.setLength(0);
				sql.append(" delete from "+ nmTabela+"  where cd_segmento =").append(cols[0]).append(" and cd_area= ").append(cols[1]);
				sql.append(" and cd_microarea = ").append(cols[2]).append(" and nr_familia =").append(cols[3]).append(" and cd_ano=").append(cols[4]);
				sql.append(" and cd_paciente = ").append(cols[5]).append(" and cd_mes = ").append(cols[6]);
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	//**************************************UPDATE************************************************
	public void updateFichaCadFam(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and  cd_ano= ").append(colsKey[4]);				

				sqlNotKey.append(" ds_endereco='").append(colsNotKey[0]).append("', num_endereco=").append(colsNotKey[1]);
				sqlNotKey.append(", ds_bairro='").append(colsNotKey[2]).append("', ds_cep='").append(colsNotKey[3]);	
				sqlNotKey.append("', cd_municipio=").append(colsNotKey[4]).append(", dt_cadastro='").append(colsNotKey[5]);
				sqlNotKey.append("', cd_uf='").append(colsNotKey[6]).append("', cd_tpcasa=").append(colsNotKey[7]);
				sqlNotKey.append(", ds_tpcasaesp='").append(colsNotKey[8]).append("', num_comodos=").append(colsNotKey[9]);
				sqlNotKey.append(", fl_energia='").append(colsNotKey[10]).append("', cd_destlixo=").append(colsNotKey[11]);
				sqlNotKey.append(", cd_trataagua=").append(colsNotKey[12]).append(", cd_abasteagua=").append(colsNotKey[13]);
				sqlNotKey.append(", cd_fezesurina=").append(colsNotKey[14]).append(", cd_doencaprocu=").append(colsNotKey[15]);
				sqlNotKey.append(", ds_doencaprocuesp='").append(colsNotKey[16]).append("', cd_grupocomu=").append(colsNotKey[17]);
				sqlNotKey.append(", ds_grupocomuesp='").append(colsNotKey[18]).append("', cd_meioscomuni=").append(colsNotKey[19]);
				sqlNotKey.append(", ds_meioscomuniesp='").append(colsNotKey[20]).append("', cd_meiostrans=").append(colsNotKey[21]);
				sqlNotKey.append(", ds_meiostransesp='").append(colsNotKey[22]).append("', fl_possuiplan='").append(colsNotKey[23]);
				sqlNotKey.append("', num_pesplan=").append(colsNotKey[24]).append(", nm_plano='").append(colsNotKey[25]);
				sqlNotKey.append("', ds_observacao='").append(colsNotKey[26]).append("'");
				
				sql.append(" update fichacadfam  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updatePacFamilia(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" nm_paciente='").append(colsNotKey[0]).append("', dt_nascimento='").append(colsNotKey[1]);
				sqlNotKey.append("', num_idade=").append(colsNotKey[2]).append(", fl_sexo='").append(colsNotKey[3]);	
				sqlNotKey.append("', fl_menorquinze='").append(colsNotKey[4]).append("', fl_alfabetizado='").append(colsNotKey[5]);
				sqlNotKey.append("', fl_freqescola='").append(colsNotKey[6]).append("', cd_ocupacao=").append(colsNotKey[7]);
				sqlNotKey.append(", fl_alc='").append(colsNotKey[8]).append("', fl_cha='").append(colsNotKey[9]);
				sqlNotKey.append("', fl_def='").append(colsNotKey[10]).append("', fl_dia='").append(colsNotKey[11]);
				sqlNotKey.append("', fl_dme='").append(colsNotKey[12]).append("', fl_epi='").append(colsNotKey[13]);
				sqlNotKey.append("', fl_ges='").append(colsNotKey[14]).append("', fl_han='").append(colsNotKey[15]);
				sqlNotKey.append("', fl_ha='").append(colsNotKey[16]).append("', fl_mal='").append(colsNotKey[17]);
				sqlNotKey.append("', fl_tbc='").append(colsNotKey[18]).append("', fl_maior='").append(colsNotKey[19]).append("'");
				
				sql.append(" update pacfamilia  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updateFichaTb(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" num_comunicantes=").append(colsNotKey[0]).append(", num_comunicantes5=").append(colsNotKey[1]);
				sqlNotKey.append(", ds_observacao='").append(colsNotKey[2]).append("'");	
				
				sql.append(" update fichatb  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void updateFichaTbAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sqlNotKey.append(" dt_visita='").append(colsNotKey[0]).append("', fl_medicacao='").append(colsNotKey[1]);
				sqlNotKey.append("', fl_reaindese='").append(colsNotKey[2]);
				sqlNotKey.append("', dt_consulta='").append(colsNotKey[3]).append("', fl_escarro='").append(colsNotKey[4]);
				sqlNotKey.append("', num_examinados=").append(colsNotKey[5]).append(", num_combcg=").append(colsNotKey[6]);	
				
				sql.append(" update fichatbacomp  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updateFichaHan(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" num_comunicantes=").append(colsNotKey[0]).append(", ds_observacao='").append(colsNotKey[1]).append("'");
								
				sql.append(" update fichahan  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void updateFichaHanAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sqlNotKey.append(" dt_visita='").append(colsNotKey[0]).append("', fl_medicacao='").append(colsNotKey[1]);
				sqlNotKey.append("', dt_ultdose='").append(colsNotKey[2]).append("', fl_autocuidados='").append(colsNotKey[3]);
				sqlNotKey.append("', dt_consulta='").append(colsNotKey[4]).append("', num_examinados=").append(colsNotKey[5]);
				sqlNotKey.append(", num_combcg=").append(colsNotKey[6]);	
				
				sql.append(" update fichahanacomp  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updateFichaHa(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" fl_fumante='").append(colsNotKey[0]).append("', ds_observacao='").append(colsNotKey[1]).append("'");
								
				sql.append(" update fichaha  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void updateFichaHaAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sqlNotKey.append(" dt_visita='").append(colsNotKey[0]).append("', fl_dieta='").append(colsNotKey[1]);
				sqlNotKey.append("', fl_medicacao='").append(colsNotKey[2]).append("', fl_exercicio='").append(colsNotKey[3]);
				sqlNotKey.append("', ds_pressao='").append(colsNotKey[4]).append("', dt_ultconsulta='").append(colsNotKey[5]);
				sqlNotKey.append("'");
				
				sql.append(" update fichahaacomp  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updateFichaDia(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" ds_observacao='").append(colsNotKey[0]).append("'");
								
				sql.append(" update fichadia  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void updateFichaDiaAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sqlNotKey.append(" dt_visita='").append(colsNotKey[0]).append("', fl_dieta='").append(colsNotKey[1]);
				sqlNotKey.append("', fl_exercicio='").append(colsNotKey[2]).append("', fl_insulina='").append(colsNotKey[3]);
				sqlNotKey.append("', fl_hipoglicemiante='").append(colsNotKey[4]).append("', dt_ultconsulta='").append(colsNotKey[5]);
				sqlNotKey.append("'");
				
				sql.append(" update fichadiaacomp  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updateFichaGes(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" dt_ultregra='").append(colsNotKey[0]).append("', dt_parto='").append(colsNotKey[1]);
				sqlNotKey.append("', dt_vacina1='").append(colsNotKey[2]).append("', dt_vacina2='").append(colsNotKey[3]);
				sqlNotKey.append("', dt_vacina3='").append(colsNotKey[4]).append("', dt_vacinar='").append(colsNotKey[5]);
				sqlNotKey.append("', fl_seisgesta='").append(colsNotKey[6]).append("', fl_natimorto='").append(colsNotKey[7]);
				sqlNotKey.append("', fl_36anosmais='").append(colsNotKey[9]).append("', fl_menos20='").append(colsNotKey[9]);
				sqlNotKey.append("', fl_sangramento='").append(colsNotKey[10]).append("', fl_edema='").append(colsNotKey[11]);
				sqlNotKey.append("', fl_diabetes='").append(colsNotKey[12]).append("', fl_pressaoalta='").append(colsNotKey[13]);
				sqlNotKey.append("', dt_nascvivo='").append(colsNotKey[14]).append("', dt_nascmorto='").append(colsNotKey[15]);
				sqlNotKey.append("', dt_aborto='").append(colsNotKey[16]).append("', dt_puerperio1='").append(colsNotKey[17]);
				sqlNotKey.append("', dt_puerperio2='").append(colsNotKey[18]).append("', ds_observacao='").append(colsNotKey[19]);
				sqlNotKey.append("'");
								
				sql.append(" update fichages  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void updateFichaGesAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sqlNotKey.append(" fl_estadonutri='").append(colsNotKey[0]).append("', dt_consprenatal='").append(colsNotKey[1]);
				sqlNotKey.append("', dt_visita='").append(colsNotKey[2]).append("'");
				
				sql.append(" update fichagesacomp  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updateFichaIdoso(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" fl_fumante='").append(colsNotKey[0]).append("', dt_dtp='").append(colsNotKey[1]);
				sqlNotKey.append("', dt_influenza='").append(colsNotKey[2]).append("', dt_pneumonococos='").append(colsNotKey[3]);
				sqlNotKey.append("', fl_hipertensao='").append(colsNotKey[4]).append("', fl_tuberculose='").append(colsNotKey[5]);
				sqlNotKey.append("', fl_alzheimer='").append(colsNotKey[6]).append("', fl_deffisica='").append(colsNotKey[7]);
				sqlNotKey.append("', fl_hanseniase='").append(colsNotKey[9]).append("', fl_malparkson='").append(colsNotKey[9]);
				sqlNotKey.append("', fl_cancer='").append(colsNotKey[10]).append("', fl_acamado='").append(colsNotKey[11]);
				sqlNotKey.append("', fl_internamentos='").append(colsNotKey[12]).append("', fl_sequelasavc='").append(colsNotKey[13]);
				sqlNotKey.append("', fl_alcolatra='").append(colsNotKey[14]).append("'");
								
				sql.append(" update fichaidoso  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void updateFichaIdosoAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sqlNotKey.append(" fl_estadonutri='").append(colsNotKey[0]).append("', dt_visita='").append(colsNotKey[1]);
				sqlNotKey.append("', dt_acomppsf='").append(colsNotKey[2]).append("'");
				
				sql.append(" update fichaidosoacomp  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void updateFichaCrianca(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				

				sqlNotKey.append(" cd_avaliacaoalimen=").append(colsNotKey[0]).append(", ds_tipoalimentacao='").append(colsNotKey[1]);
				sqlNotKey.append("', fl_imunizado='").append(colsNotKey[2]).append("', fl_completaresq='").append(colsNotKey[3]);
				sqlNotKey.append("', fl_diarreia='").append(colsNotKey[4]).append("', fl_usaramtro='").append(colsNotKey[5]);
				sqlNotKey.append("', fl_internamentos='").append(colsNotKey[6]).append("', fl_ira='").append(colsNotKey[7]);
				sqlNotKey.append("', fl_caxumba='").append(colsNotKey[9]).append("', fl_catapora='").append(colsNotKey[9]);
				sqlNotKey.append("', fl_baixopesonasc='").append(colsNotKey[10]).append("', fl_deficiencias='").append(colsNotKey[11]);
				sqlNotKey.append("'");
								
				sql.append(" update fichacrianca  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void updateFichaCriancaAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sqlNotKey.append(" dt_quadronutri='").append(colsNotKey[0]).append("', num_peso=").append(colsNotKey[1]);
				sqlNotKey.append(", num_altura=").append(colsNotKey[2]);
				
				sql.append(" update fichacriancaacomp  set ").append(sqlNotKey.toString()).append(sqlKey.toString());
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
//	*************************************************INSERT*********************************************
	public void insertFichaCadFam(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);				
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and  cd_ano= ").append(colsKey[4]);				
				
				sql.append(" insert into fichacadfam ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_ano,");
				sql.append(" ds_endereco,num_endereco,ds_bairro,ds_cep,cd_municipio,dt_cadastro,cd_uf,cd_tpcasa,");
				sql.append(" ds_tpcasaesp,num_comodos,fl_energia,cd_destlixo,cd_trataagua,cd_abasteagua,cd_fezesurina,");
				sql.append(" cd_doencaprocu,ds_doencaprocuesp,cd_grupocomu,ds_grupocomuesp,cd_meioscomuni,ds_meioscomuniesp,");
				sql.append(" cd_meiostrans,ds_meiostransesp,fl_possuiplan,num_pesplan,nm_plano,ds_observacao)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]);
				sql.append(",").append(colsKey[2]).append(",").append(colsKey[3]).append(",").append(colsKey[4]);
				sql.append(",'").append(colsNotKey[0]).append("',").append(colsNotKey[1]).append(",'").append(colsNotKey[2]);
				sql.append("','").append(colsNotKey[3]).append("',").append(colsNotKey[4]).append(",'").append(colsNotKey[5]);
				sql.append("','").append(colsNotKey[6]).append("',").append(colsNotKey[7]).append(",'").append(colsNotKey[8]);
				sql.append("',").append(colsNotKey[9]).append(",'").append(colsNotKey[10]).append("',").append(colsNotKey[11]);
				sql.append(",").append(colsNotKey[12]).append(",").append(colsNotKey[13]).append(",").append(colsNotKey[14]);
				sql.append(",").append(colsNotKey[15]).append(",'").append(colsNotKey[16]).append("',").append(colsNotKey[17]);
				sql.append(",'").append(colsNotKey[18]).append("',").append(colsNotKey[19]).append(",'").append(colsNotKey[20]);
				sql.append("',").append(colsNotKey[21]).append(",'").append(colsNotKey[22]).append("','").append(colsNotKey[23]);
				sql.append("',").append(colsNotKey[24]).append(",'").append(colsNotKey[25]).append("','").append(colsNotKey[26]);
				sql.append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichacadfam ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertPacFamilia(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);				
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				
				
				sql.append(" insert into pacfamilia ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,");
				sql.append(" nm_paciente,dt_nascimento,num_idade,fl_sexo,fl_menorquinze,fl_alfabetizado,fl_freqescola,cd_ocupacao,");
				sql.append(" fl_alc,fl_cha,fl_def,fl_dia,fl_dme,fl_epi,fl_ges,fl_han,fl_ha,fl_mal,fl_tbc,fl_maior)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]);
				sql.append(",").append(colsKey[2]).append(",").append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]);
				sql.append(",'").append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("',").append(colsNotKey[2]).append("");
				sql.append(",'").append(colsNotKey[3]).append("','").append(colsNotKey[4]).append("','").append(colsNotKey[5]);
				sql.append("','").append(colsNotKey[6]).append("',").append(colsNotKey[7]).append(",'").append(colsNotKey[8]);
				sql.append("','").append(colsNotKey[9]).append("','").append(colsNotKey[10]).append("','").append(colsNotKey[11]);
				sql.append("','").append(colsNotKey[12]).append("','").append(colsNotKey[13]).append("','").append(colsNotKey[14]);
				sql.append("','").append(colsNotKey[15]).append("','").append(colsNotKey[16]).append("','").append(colsNotKey[17]);
				sql.append("','").append(colsNotKey[18]).append("','").append(colsNotKey[19]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM pacfamilia ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertFichaTb(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);				
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				
				
				sql.append(" insert into fichatb  ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,num_comunicantes,");
				sql.append("num_comunicantes5,ds_observacao) select ").append(colsKey[0]).append(",").append(colsKey[1]);
				sql.append(",").append(colsKey[2]).append(",").append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]);
				sql.append(",").append(colsNotKey[0]).append(",").append(colsNotKey[1]).append(",'").append(colsNotKey[2]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichatb ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertFichaTbAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
					
				
				sql.append(" insert into  fichatbacomp  (  cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_mes,cd_ano,");
				sql.append(" dt_visita,fl_medicacao,fl_reaindese,dt_consulta,fl_escarro,num_examinados,num_combcg) ");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsKey[6]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("','").append(colsNotKey[3]);
				sql.append("','").append(colsNotKey[4]).append("',").append(colsNotKey[5]).append(",").append(colsNotKey[6]);
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichatbacomp ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertFichaHan(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				
								
				sql.append(" insert into fichahan  ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,num_comunicantes,ds_observacao) ");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsNotKey[0]);
				sql.append(",'").append(colsNotKey[1]).append("' ");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichahan ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void insertFichaHanAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);		
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sql.append(" insert into  fichahanacomp  (  cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_mes,cd_ano,");
				sql.append(" dt_visita,fl_medicacao,dt_ultdose,fl_autocuidados,dt_consulta,num_examinados,num_combcg )");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsKey[6]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("','");
				sql.append(colsNotKey[3]).append("','").append(colsNotKey[4]).append("',").append(colsNotKey[5]).append(",");
				sql.append(colsNotKey[6]);
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichahanacomp ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertFichaHa(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);				
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				
				
				sql.append(" insert into fichaha  ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,fl_fumante,ds_observacao)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichaha ").append(sqlKey.toString()).append(")");
						
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void insertFichaHaAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sql.append(" insert into  fichahaacomp  (  cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_mes,cd_ano,");
				sql.append(" dt_visita,fl_dieta,fl_medicacao,fl_exercicio,ds_pressao,dt_ultconsulta)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsKey[6]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("','");
				sql.append(colsNotKey[3]).append("','").append(colsNotKey[4]).append("','").append(colsNotKey[5]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichahaacomp ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}	
	
	public void insertFichaDia(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);				
								
				sql.append(" insert into fichadia  ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,ds_observacao)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",'");
				sql.append(colsNotKey[0]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichadia ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void insertFichaDiaAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);
				
				sql.append(" insert into  fichadiaacomp  (  cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_mes,cd_ano,");
				sql.append(" dt_visita,fl_dieta,fl_exercicio,fl_insulina,fl_hipoglicemiante,dt_ultconsulta )");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsKey[6]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("','");
				sql.append(colsNotKey[3]).append("','").append(colsNotKey[4]).append("','").append(colsNotKey[5]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichadiaacomp ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertFichaGes(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);
				
				sql.append(" insert into fichages  ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,");
				sql.append(" dt_ultregra,dt_parto,dt_vacina1,dt_vacina2,dt_vacina3,dt_vacinar,fl_seisgesta,fl_natimorto,fl_36anosmais,");
				sql.append(" fl_menos20,fl_sangramento,fl_edema,fl_diabetes,fl_pressaoalta,dt_nascvivo,dt_nascmorto,dt_aborto,");
				sql.append(" dt_puerperio1,dt_puerperio2,ds_observacao)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("','");
				sql.append(colsNotKey[3]).append("','").append(colsNotKey[4]).append("','").append(colsNotKey[5]).append("','");
				sql.append(colsNotKey[6]).append("','").append(colsNotKey[7]).append("','").append(colsNotKey[8]).append("','");
				sql.append(colsNotKey[9]).append("','").append(colsNotKey[10]).append("','").append(colsNotKey[11]).append("','");
				sql.append(colsNotKey[12]).append("','").append(colsNotKey[13]).append("','").append(colsNotKey[14]).append("','");
				sql.append(colsNotKey[15]).append("','").append(colsNotKey[16]).append("','").append(colsNotKey[17]).append("','");
				sql.append(colsNotKey[18]).append("','").append(colsNotKey[19]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichages ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void insertFichaGesAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);			
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);				
				
				sql.append(" insert into  fichagesacomp  (  cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_mes,cd_ano,");
				sql.append(" fl_estadonutri,dt_consprenatal,dt_visita)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsKey[6]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichagesacomp ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertFichaIdoso(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			StringBuffer sqlNotKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				sqlNotKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);
								
				sql.append(" insert into fichaidoso  ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,");
				sql.append(" fl_fumante,dt_dtp,dt_influenza,dt_pneumonococos,fl_hipertensao,fl_tuberculose,fl_alzheimer,fl_deffisica,");
				sql.append(" fl_hanseniase,fl_malparkson,fl_cancer,fl_acamado,fl_internamentos,fl_sequelasavc,fl_alcolatra)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("','");
				sql.append(colsNotKey[3]).append("','").append(colsNotKey[4]).append("','").append(colsNotKey[5]).append("','");
				sql.append(colsNotKey[6]).append("','").append(colsNotKey[7]).append("','").append(colsNotKey[8]).append("','");
				sql.append(colsNotKey[9]).append("','").append(colsNotKey[10]).append("','").append(colsNotKey[11]).append("','");
				sql.append(colsNotKey[12]).append("','").append(colsNotKey[13]).append("','").append(colsNotKey[14]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichaidoso ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void insertFichaIdosoAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);				
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);	
				
				sql.append(" insert into  fichaidosoacomp  (  cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_mes,cd_ano,");
				sql.append(" fl_estadonutri,dt_visita,dt_acomppsf)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsKey[6]).append(",'");
				sql.append(colsNotKey[0]).append("','").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("'");
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichaidosoacomp ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	public void insertFichaCrianca(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);			
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);				
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_ano= ").append(colsKey[5]);		
				
				sql.append(" insert into fichacrianca ( cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_ano,");
				sql.append(" cd_avaliacaoalimen,ds_tipoalimentacao,fl_imunizado,fl_completaresq,fl_diarreia,fl_usaramtro,fl_internamentos,");
				sql.append(" fl_ira,fl_caxumba,fl_catapora,fl_baixopesonasc,fl_deficiencias)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",");
				sql.append(colsNotKey[0]).append(",'").append(colsNotKey[1]).append("','").append(colsNotKey[2]).append("','");
				sql.append(colsNotKey[3]).append("','").append(colsNotKey[4]).append("','").append(colsNotKey[5]).append("','");
				sql.append(colsNotKey[6]).append("','").append(colsNotKey[7]).append("','").append(colsNotKey[8]).append("','");
				sql.append(colsNotKey[9]).append("','").append(colsNotKey[10]).append("','").append(colsNotKey[11]).append("'");				
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichacrianca ").append(sqlKey.toString()).append(")");
				
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}

	public void insertFichaCriancaAcomp(String[] rsUpKey,String[] rsUpNotKey) throws Exception{		
		String[] colsKey=null;
		String[] colsNotKey=null;
		Connection connection =null;
		try {
			 connection = beginTransaction();		
			Statement stm = connection.createStatement();
			StringBuffer sql = new StringBuffer(1024);
			StringBuffer sqlKey = new StringBuffer(1024);
			for(int i=0;i<rsUpKey.length;i++){
				colsKey=Convert.tokenizeString(rsUpKey[i],'|');	
				colsNotKey=Convert.tokenizeString(rsUpNotKey[i],'|');	
				sql.setLength(0);
				sqlKey.setLength(0);
				
				sqlKey.append(" where cd_segmento =").append(colsKey[0]).append(" and cd_area= ").append(colsKey[1]);
				sqlKey.append(" and cd_microarea = ").append(colsKey[2]).append(" and nr_familia =").append(colsKey[3]);
				sqlKey.append(" and cd_paciente=").append(colsKey[4]).append(" and  cd_mes= ").append(colsKey[5]);				
				sqlKey.append(" and  cd_ano= ").append(colsKey[6]);				
				
				sql.append(" insert into  fichacriancaacomp  (  cd_segmento,cd_area,cd_microarea,nr_familia,cd_paciente,cd_mes,cd_ano,");
				sql.append(" dt_quadronutri,num_peso,num_altura)");
				sql.append(" select ").append(colsKey[0]).append(",").append(colsKey[1]).append(",").append(colsKey[2]).append(",");
				sql.append(colsKey[3]).append(",").append(colsKey[4]).append(",").append(colsKey[5]).append(",").append(colsKey[6]).append(",'");
				sql.append(colsNotKey[0]).append("',").append(colsNotKey[1]).append(",").append(colsNotKey[2]);
				sql.append(" WHERE NOT EXISTS( SELECT * FROM fichacriancaacomp ").append(sqlKey.toString()).append(")");
				System.out.println(sql);
				stm.executeUpdate(sql.toString());	
			}
			commitTransaction(connection);
			stm.close();			   
		} catch (Exception e) {		
			rollbackTransaction(connection);
			throw new Exception(e);
		}	
		finally{
			closeConnection(connection);
		}		
	}
	
	/*public static void main(String[] args) {
		UpSincBusiness  apmBusiness =  new UpSincBusiness();
		apmBusiness.setRegistroSincronizado(16,"OCUPACAO","I","6626446");
	}*/
	
}

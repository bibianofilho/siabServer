
import br.com.serverAPM.persistence.DownSincBusiness;
import br.com.serverAPM.persistence.UpSincBusiness;


public class Handler
{
   
   
   public String[] getOcupacao(int cdAgente, String tpOperacao)throws Exception {
	   System.out.println(""+cdAgente+" getOcupacao "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();	   
	   return downSincBusiness.getOcupacao(cdAgente,tpOperacao); 	 	 
   }  
   
   public String[] getFichaCadFam(int cdAgente, String tpOperacao) throws Exception {
	   System.out.println(""+cdAgente+" getFichaCadFam "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaCadFam(cdAgente,tpOperacao); 	 	 
   }
   
   public String[] getPacFamilia(int cdAgente, String tpOperacao) throws Exception {
	   System.out.println(""+cdAgente+" getPacFamilia "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getPacFamilia(cdAgente,tpOperacao); 	 	 
   }
   
   public String[] getFichaTb(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaTb "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaTb(cdAgente,tpOperacao); 	 	 
   }
   
   public String[] getFichaTbAcomp(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaTbAcomp "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaTbAcomp(cdAgente,tpOperacao); 	 	 
   }
   
   public String[] getFichaHan(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaHan "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaHan(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaHanAcomp(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaHanAcomp "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaHanAcomp(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaHa(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaHa "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaHa(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaHaAcomp(int cdAgente, String tpOperacao)throws Exception {
	   System.out.println(""+cdAgente+" getFichaHaAcomp "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaHaAcomp(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaDia(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaDia "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaDia(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaDiaAcomp(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaDiaAcomp "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaDiaAcomp(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaGes(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaGes "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaGes(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaGesAcomp(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaGesAcomp "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaGesAcomp(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaCri(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaCri "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaCri(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaCriAcomp(int cdAgente, String tpOperacao)throws Exception {
	   System.out.println(""+cdAgente+" getFichaCriAcomp "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaCriAcomp(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaIdoso(int cdAgente, String tpOperacao)throws Exception {
	   System.out.println(""+cdAgente+" getFichaIdoso "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaIdoso(cdAgente,tpOperacao); 	 	 
   }
   public String[] getFichaIdosoAcomp(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getFichaIdosoAcomp "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getFichaIdosoAcomp(cdAgente,tpOperacao); 	 	 
   }
   public String[] getAgente(int cdAgente, String tpOperacao) throws Exception{
	   System.out.println(""+cdAgente+" getAgente "+tpOperacao);
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.getAgente(cdAgente,tpOperacao); 	 	 
   }
   public String[] validateAgente(int cdAgente) throws Exception{
	   System.out.println("****************Agente = "+cdAgente+" ****************************");
	   DownSincBusiness  downSincBusiness =  new DownSincBusiness();
	   return downSincBusiness.validateAgente(cdAgente); 	 	 
   }
   
   
   //*********************************DT_SINCRONIMO****************************************
   public void setSinc(int cdAgente, String nmTabela, String tpOperacao,String cdSincUsuario)throws Exception {
	   System.out.println(""+cdAgente+"|"+nmTabela+"|"+tpOperacao+"|"+cdSincUsuario);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.setRegistroSincronizado( cdAgente, nmTabela, tpOperacao, cdSincUsuario); 	 	 
   }
   
   //******************************************DELETE*********************************************************
   public void deleteFicha(int cdAgente,String[]rsDel,String nmTabela) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsDel.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.deleteFicha( rsDel, nmTabela); 	 	 
   }
   
   public void deleteFichaAcomp(int cdAgente,String[]rsDel,String nmTabela) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsDel.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.deleteFichaAcomp(  rsDel, nmTabela); 	 	 
   }
   
   //***************************************UPDATE***********************************************************
   public void updateFichaCadFam(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaCadFam( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updatePacFamilia(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updatePacFamilia( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaTb(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaTb( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaTbAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaTbAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaHan(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaHan( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaHanAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaHanAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaHa(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaHa( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaHaAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaHaAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaDia(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaDia( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaDiaAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaDiaAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaGes(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaGes( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaGesAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaGesAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaIdoso(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaIdoso( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaIdosoAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaIdosoAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaCrianca(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaCrianca( rsUpKey, rsUpNotKey); 	 	 
   }
   public void updateFichaCriancaAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.updateFichaCriancaAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   //*************************************************INSERT*********************************************
   
   public void insertFichaCadFam(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaCadFam( rsUpKey, rsUpNotKey); 	 	 
   }
   
   public void insertPacFamilia(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertPacFamilia( rsUpKey, rsUpNotKey); 	 	 
   }
   
   public void insertFichaTb(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaTb( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaTbAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaTbAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   
   public void insertFichaHan(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaHan( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaHanAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaHanAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   
   public void insertFichaHa(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaHa( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaHaAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaHaAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   
   public void insertFichaDia(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaDia( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaDiaAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaDiaAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   
   public void insertFichaGes(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaGes( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaGesAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaGesAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaIdoso(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaIdoso( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaIdosoAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaIdosoAcomp( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaCrianca(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaCrianca( rsUpKey, rsUpNotKey); 	 	 
   }
   public void insertFichaCriancaAcomp(int cdAgente, String nmTabela, String[] rsUpKey, String[] rsUpNotKey) throws Exception {
	   System.out.println(""+cdAgente+" "+nmTabela+" "+rsUpKey.length);
	   UpSincBusiness upSincBusiness = new UpSincBusiness();	   
	   upSincBusiness.insertFichaCriancaAcomp( rsUpKey, rsUpNotKey); 	 	 
   } 
   
}

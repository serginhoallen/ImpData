#Include "Protheus.ch"
#Include "TopConn.ch"
// ------------------------------------------------------------------------
// |  Desenvolvido por: SÈrgio OrtÛria Simıes                             |
// |  Data: 14/05/2025                                                    |
// |  Ultimo ajuste: 16/05/2025                                           |
// |  "Esse fonte levou mais tempo do que deveria."                       |
// |  Linkedin: http://shre.ink/sergiosimoes                              | 
// ------------------------------------------------------------------------
User Function impdata()

	Local   aArea     := GetArea()
	Local   cPerg     := PadR("X_ZAPPEND", 10)
	Private cDBOrigem := ""
	Private cTabDe    := ""
	Private cTabAt    := ""

	//Se a pergunta for confirmada (com o schema de origem, ex.: P11)
	If Pergunte(cPerg, .T.)
		cDbOrigem := MV_PAR01
		cTabDe    := MV_PAR02
		cTabAt    := MV_PAR03

		//Chama a rotina de cÛpia
		If !Empty(cDbOrigem)
			Processa({|| importa()}, "Processando...")
		EndIf
	EndIf

	RestArea(aArea)
Return

Static Function importa()

	Local cQry    := ""
	Local cQryDad := ""
	Local cQuery  := ""
	Local cQueryBin  := ""
	Local cQueryVar  := ""
	Local cNotExist  := ""
	Local cAliasNew  := GetNextAlias()
	Local cAliasnew2  := GetNextAlias()
	Local cAliasnew3 := GetNextAlias()
	Local nTotal  := 0
	Local nAtual  := 0
	Local ll      := 0
	Local nRec    := 0
	Local NERRO   := 0
	Local lIdent  := .F.
	Local cBase   := cDbOrigem
	Local cAliAtu := ""
	local cTabGrup := ""
	Local aTabC   := {}
	Local cLogErr := ""
	local cSQLRec1  := ", D_E_L_E_T_, R_E_C_N_O_, R_E_C_D_E_L_"

// Define para qual tabela ele vai enviar os arquivos de acordo com o grupo
	IF Alltrim(SM0->M0_CODIGO) == "01"
		cTabGrup := "010"
	ENDIF

	IF Alltrim(SM0->M0_CODIGO) == "02"
		cTabGrup := "020"
	ENDIF

// Valida caso esteja selecionando o banco do 01 para importar para o 02, ele para o processo
	IF Alltrim(SM0->M0_CODIGO) == "02" .AND. ALLTRIM(cBase) == "CPE7XC_129811_PR_PD_23772444"
		ALERT("Voce esta importando a base tcloud pro grupo errado")
		return
	ENDIF

	IF Alltrim(SM0->M0_CODIGO) == "01" .AND. ALLTRIM(cBase) == "PROTHEUS_HOMOLOG"
		ALERT("Voce esta importando a base Local pro grupo errado")
		return
	ENDIF

	//Faz consulta das tabelas que tem dados
	cQry += " SELECT    " + CRLF
	cQry += "     esquemas.Name AS Esquema_Nome,   " + CRLF
	cQry += "     tabelas.Name AS Tabela_Nome,   " + CRLF
	cQry += "     particoes.Rows AS Numero_Linhas,   " + CRLF
	cQry += "     SUM(alocacao.total_pages) * 8 AS Espaco_KB_Total,   " + CRLF
	cQry += "     SUM(alocacao.used_pages) * 8 AS Espaco_KB_Usado,   " + CRLF
	cQry += "     (SUM(alocacao.total_pages) - SUM(alocacao.used_pages)) * 8 AS Espaco_KB_Sem_Uso   " + CRLF
	cQry += " FROM    " + CRLF
	cQry += "     "+ cBase +" .sys.tables tabelas   " + CRLF
	cQry += "     INNER JOIN "+cBase+".sys.schemas esquemas ON esquemas.schema_id = tabelas.schema_id   " + CRLF
	cQry += "     INNER JOIN "+cBase+".sys.indexes indices ON tabelas.OBJECT_ID = indices.object_id   " + CRLF
	cQry += "     INNER JOIN "+cBase+".sys.partitions particoes ON indices.object_id = particoes.OBJECT_ID AND indices.index_id = particoes.index_id   " + CRLF
	cQry += "     INNER JOIN "+cBase+".sys.allocation_units alocacao ON particoes.partition_id = alocacao.container_id   " + CRLF
	cQry += " WHERE    " + CRLF
	cQry += " tabelas.Name NOT LIKE 'dt%' " + CRLF
	cQry += " AND tabelas.is_ms_shipped = 0 " + CRLF
	cQry += " AND indices.OBJECT_ID > 255 " + CRLF
	cQry += " AND alocacao.used_pages != 0 " + CRLF
	cQry += " AND particoes.Rows > 0 -- << Aqui est· o novo filtro importante " + CRLF
	cQry += " AND tabelas.NAME >= '"+cTabDe+"010' " + CRLF
	cQry += " AND tabelas.NAME <= '"+cTabAt+"010' " + CRLF
	cQry += " AND LEN(tabelas.Name) = 6 " + CRLF
	cQry += " OR LEN(tabelas.Name) = 3 " + CRLF

	cQry += " GROUP BY    " + CRLF
	cQry += "     tabelas.Name, esquemas.Name, particoes.Rows   " + CRLF
	cQry += " ORDER BY    " + CRLF
	cQry += "     Esquema_Nome, Tabela_Nome   " + CRLF

	TCQuery cQry New Alias "QRY_ORI"
	Count To nTotal
	nAtual := 0
	ProcRegua(nTotal)

	Begin Transaction
		QRY_ORI->(DbGoTop())
		While ! QRY_ORI->(EoF())
			nRec := QRY_ORI->(RecNo())                    // Pega o recno para se posicionar na tabela novamente
			nAtual++
			cAliAtu := SubStr(QRY_ORI->Tabela_Nome, 1, 3) // Pega o nome do alias da tabela
			cCampos := TableFields(cAliAtu,1,",")         // Pega os campos da tabela
			IncProc("Processando "+cAliAtu+" ("+cValToChar(nAtual)+" de "+cValToChar(nTotal)+")...")
			If !("SX" $ cAliAtu)  // Valida se a tabela nùo ù uma tabela SX (Dicionario)
				cDropQry := "TRUNCATE TABLE "+cAliAtu+cTabGrup+" "                                          //                                                                                                     //
				nErro := Tcsqlexec(cDropQry) //entra na query                                                  //                                                                                                //
				If nErro <> 0                                                                                  //                                                                //
					Conout("Erro na execuÁ„o da query: "+TcSqlError(), "AtenÁ„o")                          //                                                                                                                    //
					MsgInfo("Erro na execuÁ„o da query: "+TcSqlError(), "AtenÁ„o")                         //                                                                                                                     //
					DisarmTransaction()                                                                        //
					QRY_ORI->(DbCloseArea())                                                                   //
					return                                                                                     //                                                                          //
				EndIf                                                                                          //                                                        //
//--------------------------------------------------------------------------------------------------------------------------------------------------------------------
//            Verifica se a tabela precisa fazer o SET IDENTITY_INSERT para a identificaùùo do RECNO no campo
				cQuery := " SELECT COUNT(COLUMN_NAME) AS FAZIDENT " + CRLF
				cQuery += " FROM INFORMATION_SCHEMA.COLUMNS " + CRLF
				cQuery += " WHERE TABLE_NAME = '"+cAliAtu+cTabGrup+"' AND COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1; " + CRLF
				DbUseArea(.T.,"TOPCONN",TcGenQry(,,cQuery),cAliasnew,.T.,.T.)
				IF (cAliasNew)->FAZIDENT == 1
					lIdent := .T.
				endif
				IF (cAliasNew)->FAZIDENT == 0
					lIdent := .F.
				endif
//-------------------------------------------------------------------------------------------------------------
//Removido do codigo pela n„o necessidade do NOT EXIST, porÈm deixei comentando para caso tenha necessidade
//-------------------------------------------------------------------------------------------------------------

				cQryDad := " BEGIN TRAN;"  + CRLF

				cQryDad += " INSERT INTO PROTHEUS_UNIFICADO.dbo."+cAliAtu+cTabGrup+"( "  + CRLF
				cQryDad +=  cCampos+cSQLRec1+")"  + CRLF
				cQryDad += " SELECT "  + CRLF
				cQryDad +=  cCampos+cSQLRec1  + CRLF
				cQryDad += " FROM " + CRLF
				cQryDad += "     "+ALLTRIM(cBase)+"."+ALLTRIM(QRY_ORI->Esquema_Nome)+"."+ALLTRIM(QRY_ORI->Tabela_Nome)+" TAB " + CRLF
				cQryDad += " WHERE "
				cQryDad += "     TAB.D_E_L_E_T_ = ' ' " + CRLF

				cQryDad += "  COMMIT; "  + CRLF      // Confirma todas as alteraùùes feitas dentro de uma transaùùo.
				cQryDad += "  CHECKPOINT;  "  + CRLF // Forùa a gravaùùo em disco das alteraùùes que estùo sù na memùria
				cQryDad += "  WAITFOR DELAY '00:00:05'; "  + CRLF // Delay para aliviar o log

				nErro := Tcsqlexec(cQryDad) // Executa a query

				CONOUT("-----------------")
				If nErro <> 0 .AND. !("PRIMARY KEY" $ SUBSTR(TcSqlError(),108,19)) // se n„o der erro e n„o for erro de chave primaria

				Else
					Conout("Erro na execuÁ„o da query: "+TcSqlError(), "AtenÁ„o")
					DisarmTransaction()
					conout("Last Range "+cAliAtu )
					QRY_ORI->(DbCloseArea())
					return
				endif

			EndIf
			if nErro == 0
				conout("Tabela " + cAliAtu + ' Importada !')
			endif
			IF nErro <> 0 .AND. ("PRIMARY KEY" $ SUBSTR(TcSqlError(),108,19))
				CONOUT(cAliAtu+" PRIMARY KEY") // diz qual tabela deu erro de chave primaria
			ENDIF
			CONOUT("-----------------")
			cAliasnew  := GetNextAlias() // Redefine as querys com nov alias
			cAliasnew2 := GetNextAlias() // Redefine as querys com nov alias
			cAliasnew3 := GetNextAlias() // Redefine as querys com nov alias
			//endif

			QRY_ORI->(DbGoTo(nRec)) // volta para o recno da query origem
			QRY_ORI->(DbSkip())     // pula pra proxima tabela de importaÁ„o
		EndDo
		QRY_ORI->(DbCloseArea())
	End Transaction

//Se houve erro
	If !Empty(cLogErr)
		Aviso('AtenÁ„o', "Houveram erros nas tabelas: "+Chr(13)+Chr(10)+cLogErr, {'Ok'}, 03)

	Else
		MsgInfo("Processo terminado!", "AtenÁ„o")
	EndIf

Return

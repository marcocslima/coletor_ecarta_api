import os
import io
import ftplib
import json
import tempfile
from base64 import b64decode

from fastapi import FastAPI, HTTPException
from pydantic_settings import BaseSettings

# Libs do Google
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

# 1. Configuração para carregar variáveis de ambiente
class Settings(BaseSettings):
    host: str
    port: int
    user_ecarta: str
    password_ecarta: str
    directory_ecarta: str
    google_folder_id: str
    google_credentials_base64: str | None = None

    class Config:
        env_file = ".env"

settings = Settings()

# 2. Inicialização do FastAPI
app = FastAPI(
    title="API de Sincronização FTP -> Google Drive",
    description="Baixa arquivos de um diretório FTP, salva no Google Drive e os remove do FTP. Inclui endpoints de manutenção.",
    version="1.4.0" # Atualizamos a versão
)

# 3. Lógica de autenticação com Google Drive
def get_drive_service():
    """Cria e retorna o serviço do Google Drive autenticado."""
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    try:
        if settings.google_credentials_base64:
            decoded_creds = b64decode(settings.google_credentials_base64)
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_creds_file:
                temp_creds_file.write(decoded_creds.decode('utf-8'))
                temp_creds_file_path = temp_creds_file.name
            creds = service_account.Credentials.from_service_account_file(
                temp_creds_file_path, scopes=SCOPES
            )
            os.remove(temp_creds_file_path)
        else:
            creds = service_account.Credentials.from_service_account_file(
                "service_account.json", scopes=SCOPES
            )
        service = build("drive", "v3", credentials=creds)
        return service
    except Exception as e:
        print(f"Erro ao autenticar com o Google: {e}")
        raise HTTPException(status_code=500, detail=f"Falha na autenticação com o Google Drive: {e}")

# 4. O endpoint principal da API de Sincronização
@app.post("/api/sync-files", tags=["Sincronização"])
async def sync_ftp_to_drive():
    """
    Endpoint que executa o processo completo de sincronização e exclusão.
    """
    # ... (O código desta função permanece o mesmo)
    transferred_files = []
    
    try:
        ftp = ftplib.FTP()
        ftp.connect(settings.host, settings.port)
        ftp.login(settings.user_ecarta, settings.password_ecarta)
        ftp.cwd(settings.directory_ecarta)
        
        filenames = ftp.nlst()
        if not filenames:
            return {"message": "Nenhum arquivo encontrado no diretório FTP.", "transferred_files": [], "deleted_from_ftp": []}

        drive_service = get_drive_service()

        for filename in filenames:
            try:
                mem_file = io.BytesIO()
                ftp.retrbinary(f'RETR {filename}', mem_file.write)
                mem_file.seek(0)
                file_metadata = {'name': filename, 'parents': [settings.google_folder_id]}
                media = MediaIoBaseUpload(mem_file, mimetype='application/octet-stream', resumable=True)
                file = drive_service.files().create(body=file_metadata, media_body=media, fields='id, name').execute()
                transferred_files.append(file.get('name'))
                print(f"Arquivo '{filename}' processado do FTP.")
            except Exception as loop_error:
                print(f"ERRO ao processar o arquivo '{filename}': {loop_error}. Pulando para o próximo.")
                continue
        ftp.quit()
    except ftplib.all_errors as e:
        raise HTTPException(status_code=500, detail=f"Erro no FTP: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro inesperado: {e}")
    return {"message": "Sincronização concluída!", "transferred_files": transferred_files}

# Endpoint de Manutenção para o Drive
@app.post("/api/delete-drive-files", tags=["Manutenção"])
async def delete_drive_files():
    """
    CUIDADO: Apaga TODOS os arquivos da pasta do Google Drive especificada.
    """
    # ... (O código desta função permanece o mesmo)
    drive_service = get_drive_service()
    folder_id = settings.google_folder_id
    deleted_files = []
    try:
        page_token = None
        while True:
            response = drive_service.files().list(q=f"'{folder_id}' in parents and trashed=false", fields="nextPageToken, files(id, name)", pageToken=page_token).execute()
            files_in_page = response.get('files', [])
            if not files_in_page: break
            for file in files_in_page:
                file_id, file_name = file.get('id'), file.get('name')
                try:
                    drive_service.files().delete(fileId=file_id).execute()
                    deleted_files.append(file_name)
                    print(f"Arquivo '{file_name}' deletado do Google Drive.")
                except HttpError as error:
                    print(f"ERRO ao deletar o arquivo '{file_name}': {error}")
            page_token = response.get('nextPageToken', None)
            if not page_token: break
        if not deleted_files:
            return {"message": "Operação concluída. Nenhum arquivo encontrado para deletar.", "folder_id": folder_id, "deleted_count": 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro durante a limpeza do Drive: {e}")
    return {"message": "Limpeza da pasta do Drive concluída.", "folder_id": folder_id, "deleted_count": len(deleted_files), "deleted_files": deleted_files}

# ====================================================================
# ENDPOINT DE MANUTENÇÃO PARA O FTP
# ====================================================================
@app.post("/api/cleanup-ftp", tags=["Manutenção"])
async def cleanup_ftp_directory():
    """
    CUIDADO: Apaga TODOS os arquivos da pasta do FTP especificada,
    sem fazer backup para o Google Drive.
    """
    deleted_files = []
    try:
        # Conecta, loga e entra no diretório
        ftp = ftplib.FTP()
        ftp.connect(settings.host, settings.port)
        ftp.login(settings.user_ecarta, settings.password_ecarta)
        ftp.cwd(settings.directory_ecarta)
        
        # Lista os arquivos
        filenames = ftp.nlst()
        if not filenames:
            ftp.quit()
            return {"message": "Nenhum arquivo encontrado no diretório FTP para limpar."}

        # Deleta cada arquivo
        for filename in filenames:
            try:
                ftp.delete(filename)
                deleted_files.append(filename)
                print(f"Arquivo '{filename}' deletado do FTP.")
            except Exception as loop_error:
                print(f"Não foi possível deletar o arquivo '{filename}': {loop_error}")
                continue # Pula para o próximo

        ftp.quit()

    except ftplib.all_errors as e:
        raise HTTPException(status_code=500, detail=f"Erro no FTP: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro inesperado: {e}")

    return {
        "message": "Limpeza do diretório FTP concluída com sucesso.",
        "deleted_count": len(deleted_files),
        "deleted_files": deleted_files
    }

# Endpoint raiz para teste
@app.get("/api", tags=["Status"])
def root():
    return {"status": "API está no ar!"}
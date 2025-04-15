using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Agregador
{
    class Agregador
    {
        static int contador = 0;
        private const string MutexIP = "127.0.0.1";
        private const int MutexPorta = 6000;
        private static string pastaEnviados;
        private static Timer _timerEnvio;
        private static readonly object _lockEnvio = new object();

        static async Task Main(string[] args)
        {
            const int portaReceber = 5000;
            TcpListener listener = new TcpListener(IPAddress.Any, portaReceber);

            // Caminhos
            string pastaProjeto = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\"));
            string pastaRecebidos = Path.Combine(pastaProjeto, "ReceivedFiles");
            pastaEnviados = Path.Combine(pastaProjeto, "SendFiles");

            Directory.CreateDirectory(pastaRecebidos);
            Directory.CreateDirectory(pastaEnviados);

            // Inicia o timer para envios automáticos (primeiro após 5 min, depois a cada 5 min)
            _timerEnvio = new Timer(EnviarFicheirosAutomaticamente, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));

            listener.Start();
            Console.WriteLine($"[Agregador] à escuta na porta {portaReceber}...");
            Console.WriteLine("Envio automático ativado - 5 em 5 minutos");
            Console.WriteLine("Pressione CTRL+C para sair...");

            try
            {
                while (true)
                {
                    TcpClient client = await listener.AcceptTcpClientAsync();
                    _ = Task.Run(() => TratarClienteComMutex(client, pastaRecebidos, pastaEnviados));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro no servidor: {ex.Message}");
            }
            finally
            {
                _timerEnvio?.Dispose();
                listener.Stop();
            }
        }

        static void EnviarFicheirosAutomaticamente(object state)
        {
            lock (_lockEnvio) // Garante que apenas um envio ocorra por vez
            {
                Console.WriteLine($"[{DateTime.Now:T}] Iniciar envio automático...");
                EnviarFicheirosParaServidor(pastaEnviados);
            }
        }

        static void TratarClienteComMutex(TcpClient client, string pastaRecebidos, string pastaEnviados)
        {
            string caminhoFicheiroProcessado = null;
            try
            {
                // Solicitar acesso ao Mutex
                using (TcpClient mutexClient = new TcpClient(MutexIP, MutexPorta))
                using (NetworkStream mutexStream = mutexClient.GetStream())
                {
                    // Enviar solicitação
                    byte[] solicitacao = Encoding.UTF8.GetBytes("SOLICITAR\n");
                    mutexStream.Write(solicitacao, 0, solicitacao.Length);

                    // Receber resposta
                    byte[] buffer = new byte[1024];
                    int bytesRead = mutexStream.Read(buffer, 0, buffer.Length);
                    string resposta = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();

                    if (resposta == "PERMITIDO")
                    {
                        Console.WriteLine("Permissão obtida do Mutex. Processar cliente...");

                        try
                        {
                            caminhoFicheiroProcessado = ProcessarCliente(client, pastaRecebidos, pastaEnviados);
                        }
                        finally
                        {
                            // Libertar mutex
                            byte[] libertar = Encoding.UTF8.GetBytes("LIBERTAR\n");
                            mutexStream.Write(libertar, 0, libertar.Length);
                            Console.WriteLine("Mutex libertado");

                            // Eliminar o ficheiro processado, se existir
                            if (caminhoFicheiroProcessado != null && File.Exists(caminhoFicheiroProcessado))
                            {
                                try
                                {
                                    File.Delete(caminhoFicheiroProcessado);
                                    Console.WriteLine($"Ficheiro processado eliminado: {Path.GetFileName(caminhoFicheiroProcessado)}");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Erro ao eliminar ficheiro processado: {ex.Message}");
                                }
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("Acesso negado pelo Mutex. Cliente será ignorado.");
                        client.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao tratar cliente com Mutex: {ex.Message}");
                client.Close();
            }
        }

        static string ProcessarCliente(TcpClient client, string pastaRecebidos, string pastaEnviados)
        {
            string caminhoFicheiro = null;
            try
            {
                using (NetworkStream stream = client.GetStream())
                using (MemoryStream ms = new MemoryStream())
                {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                        ms.Write(buffer, 0, bytesRead);

                    string conteudo = Encoding.UTF8.GetString(ms.ToArray());

                    int id = Interlocked.Increment(ref contador);
                    string nomeFicheiro = $"Recebido_{id:D3}.csv";
                    caminhoFicheiro = Path.Combine(pastaRecebidos, nomeFicheiro);

                    File.WriteAllText(caminhoFicheiro, conteudo);
                    Console.WriteLine($"Ficheiro guardado: {caminhoFicheiro}");

                    ProcessarFicheiro(caminhoFicheiro, pastaEnviados);

                    // Não envia imediatamente para o servidor - aguarda o timer
                    Console.WriteLine("Dados processados. Serão enviados no próximo ciclo automático.");
                }
                client.Close();
                return caminhoFicheiro;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar cliente: {ex.Message}");
                client.Close();
                return null;
            }
        }

        static void ProcessarFicheiro(string caminhoFicheiro, string pastaEnviados)
        {
            try
            {
                string[] linhas = File.ReadAllLines(caminhoFicheiro);

                string[] ficheiros = {
                    "temperaturaAr.csv",
                    "humidade.csv",
                    "temperaturaAgua.csv",
                    "ondulacao.csv",
                    "ph.csv"
                };

                foreach (var ficheiro in ficheiros)
                {
                    string caminho = Path.Combine(pastaEnviados, ficheiro);
                    if (!File.Exists(caminho))
                    {
                        File.WriteAllText(caminho, "ID,Hora,Valor\n");
                    }
                }

                for (int i = 1; i < linhas.Length; i++)
                {
                    string[] colunas = linhas[i].Split(',');
                    if (colunas.Length < 7) continue;

                    string id = colunas[0].Trim();
                    string temperaturaAr = colunas[1].Trim();
                    string humidade = colunas[2].Trim();
                    string temperaturaAgua = colunas[3].Trim();
                    string ondulacao = colunas[4].Trim();
                    string ph = colunas[5].Trim();
                    string hora = colunas[6].Trim();

                    AppendLinha(Path.Combine(pastaEnviados, "temperaturaAr.csv"), id, hora, temperaturaAr);
                    AppendLinha(Path.Combine(pastaEnviados, "humidade.csv"), id, hora, humidade);
                    AppendLinha(Path.Combine(pastaEnviados, "temperaturaAgua.csv"), id, hora, temperaturaAgua);
                    AppendLinha(Path.Combine(pastaEnviados, "ondulacao.csv"), id, hora, ondulacao);
                    AppendLinha(Path.Combine(pastaEnviados, "ph.csv"), id, hora, ph);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar ficheiro: {ex.Message}");
            }
        }

        static void AppendLinha(string caminho, string id, string hora, string valor)
        {
            try
            {
                string linha = $"{id},{hora},{valor}\n";
                File.AppendAllText(caminho, linha);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao escrever no ficheiro {caminho}: {ex.Message}");
            }
        }

        static void EnviarFicheirosParaServidor(string pastaEnviados)
        {
            string[] ficheiros = {
        "temperaturaAr.csv",
        "humidade.csv",
        "temperaturaAgua.csv",
        "ondulacao.csv",
        "ph.csv"
    };

            try
            {
                // Verifica se há dados para enviar
                bool temDados = false;
                foreach (var ficheiro in ficheiros)
                {
                    string caminho = Path.Combine(pastaEnviados, ficheiro);
                    if (File.Exists(caminho) && new FileInfo(caminho).Length > "ID,Hora,Valor\n".Length)
                    {
                        temDados = true;
                        break;
                    }
                }

                if (!temDados)
                {
                    Console.WriteLine("Nenhum dado novo para enviar.");
                    return;
                }

                using (TcpClient client = new TcpClient("127.0.0.1", 6050))
                using (NetworkStream stream = client.GetStream())
                {
                    Console.WriteLine("Conectado ao servidor. Iniciar envio de ficheiros...");

                    // 1. Enviar pedido de conexão
                    byte[] initMsg = Encoding.UTF8.GetBytes("AGREGADOR_READY\n");
                    stream.Write(initMsg, 0, initMsg.Length);

                    // 2. Esperar ACK
                    byte[] ackBuffer = new byte[1024];
                    int bytesRead = stream.Read(ackBuffer, 0, ackBuffer.Length);
                    string ack = Encoding.UTF8.GetString(ackBuffer, 0, bytesRead).Trim();

                    if (ack != "ACK")
                    {
                        Console.WriteLine("Servidor rejeitou a ligação.");
                        return;
                    }

                    Console.WriteLine("Conexão confirmada. Enviar ficheiros...");

                    // 3. Enviar cada ficheiro
                    foreach (string nomeFicheiro in ficheiros)
                    {
                        string caminhoCompleto = Path.Combine(pastaEnviados, nomeFicheiro);

                        if (!File.Exists(caminhoCompleto)) continue;

                        // Verifica se o arquivo tem conteúdo além do cabeçalho
                        string conteudo = File.ReadAllText(caminhoCompleto);
                        if (conteudo.Length <= "ID,Hora,Valor\n".Length) continue;

                        byte[] fileContent = Encoding.UTF8.GetBytes(conteudo);

                        string header = $"FILE:{nomeFicheiro}|SIZE:{fileContent.Length}\n";
                        byte[] headerBytes = Encoding.UTF8.GetBytes(header);
                        stream.Write(headerBytes, 0, headerBytes.Length);

                        stream.Write(fileContent, 0, fileContent.Length);

                        byte[] confirmBuffer = new byte[1024];
                        bytesRead = stream.Read(confirmBuffer, 0, confirmBuffer.Length);
                        string confirmacao = Encoding.UTF8.GetString(confirmBuffer, 0, bytesRead).Trim();

                        if (confirmacao == "RECEBIDO")
                        {
                            Console.WriteLine($"{nomeFicheiro} enviado com sucesso.");

                            // Limpar o arquivo mantendo apenas o cabeçalho
                            File.Delete(caminhoCompleto);
                            Console.WriteLine($"{nomeFicheiro} foi eliminado após envio bem-sucedido.");

                           // Console.WriteLine($"Conteudo do {nomeFicheiro} foi limpo, mantendo apenas o cabeçalho.");
                        }
                        else
                        {
                            Console.WriteLine($"Erro no envio de {nomeFicheiro}. Resposta: {confirmacao}");
                        }
                    }

                    byte[] endMsg = Encoding.UTF8.GetBytes("FIM_TRANSFERENCIA\n");
                    stream.Write(endMsg, 0, endMsg.Length);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao enviar ficheiros para o servidor: {ex.Message}");
            }
        }
    }
}
using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System.Security.Cryptography;

public class FileProcessingService : BackgroundService
{
    private readonly ILogger<FileProcessingService> _logger;
    private readonly string _directoryInputPath = @"C:\Users\User\Desktop\Examen Practico\archivo_in";
    private readonly string _directoryOutputPath = @"C:\Users\User\Desktop\Examen Practico\archivo_out";
    private readonly string _directoryHistoryPath = @"C:\Users\User\Desktop\Examen Practico\archivo_his";
    private readonly string _connectionString = "Server=localhost;Database=Integracion;User ID=root;Password=admin;";

    public FileProcessingService(ILogger<FileProcessingService> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting continuous file monitoring...");

        using (var watcher = new FileSystemWatcher(_directoryInputPath))
        {
            watcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite;
            watcher.Filter = "*.csv";  // Si solo deseas archivos CSV
            watcher.Created += OnFileCreated;
            watcher.Changed += OnFileCreated;
            watcher.EnableRaisingEvents = true;

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
private async void OnFileCreated(object sender, FileSystemEventArgs e)
{
    _logger.LogInformation("New file detected: {filePath}", e.FullPath);

    // Espera unos segundos para asegurarse de que el archivo esté completamente copiado
    await Task.Delay(2000);

    if (File.Exists(e.FullPath))
    {
        string fileHash = CalculateFileHash(e.FullPath);

        // Validar si el hash ya existe en la base de datos
        if (HashExistsInDatabase(fileHash))
        {
            _logger.LogInformation("File {filePath} already processed (duplicate hash). Creating duplicate summary and removing file.", e.FullPath);

            // Crear archivo de resumen para duplicado
            GenerateSummary(e.FullPath, false, "Este archivo ya fue procesado y existe en la base de datos.");

            // Eliminar el archivo original de archivo_in
            File.Delete(e.FullPath);
            return;
        }

        bool success = LoadFileToDatabase(e.FullPath, fileHash);
        if (!success)
        {
            return; // Si hubo error en la carga, no mover el archivo a históricos
        }

        GenerateSummary(e.FullPath, success);

        if (success)
        {
            MoveFileToHistory(e.FullPath);
        }
    }
    else
    {
        _logger.LogWarning("File not found: {filePath}", e.FullPath);
    }
}

private bool LoadFileToDatabase(string filePath, string fileHash)
{
    bool isSuccessful = true;
    try
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                string line;
                using (var reader = new StreamReader(filePath))
                {
                    int lineNumber = 1; // Para indicar en qué línea ocurre el error
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] data = line.Split(',');
                        string errorMessage = ValidateDataFields(data);
                        if (errorMessage != null)
                        {
                            _logger.LogWarning("Invalid data in file {filePath}, line {lineNumber}: {errorMessage}", filePath, lineNumber, errorMessage);
                            GenerateSummary(filePath, false, $"Error en línea {lineNumber}: {errorMessage}");
                            isSuccessful = false;
                            break;
                        }

                        DateTime fecha = DateTime.ParseExact(data[0], "d-M-yyyy", null);
                        int mesReporte = int.Parse(data[1]);
                        int añoReporte = int.Parse(data[2]);
                        string tipoRegistro = data[3];
                        decimal monto = decimal.Parse(data[4]);

                        string query = "INSERT INTO FlujoEfectivo (fecha, mesReporte, añoReporte, tipoRegistro, monto, hash) VALUES (@fecha, @mesReporte, @añoReporte, @tipoRegistro, @monto, @hash)";
                        using (var command = new MySqlCommand(query, connection, transaction))
                        {
                            command.Parameters.AddWithValue("@fecha", fecha);
                            command.Parameters.AddWithValue("@mesReporte", mesReporte);
                            command.Parameters.AddWithValue("@añoReporte", añoReporte);
                            command.Parameters.AddWithValue("@tipoRegistro", tipoRegistro);
                            command.Parameters.AddWithValue("@monto", monto);
                            command.Parameters.AddWithValue("@hash", fileHash); // Guardar el hash
                            command.ExecuteNonQuery();
                        }
                        lineNumber++;
                    }
                }
                if (isSuccessful)
                {
                    transaction.Commit();
                }
            }
        }
    }
    catch (Exception ex)
    {
        _logger.LogError("Error processing file {filePath}: {exception}", filePath, ex.Message);
        isSuccessful = false;
    }

    return isSuccessful;
}

private string ValidateDataFields(string[] data)
{
    if (data.Length != 5) return "El archivo no tiene todos los campos requeridos.";

    if (!DateTime.TryParseExact(data[0], "d-M-yyyy", null, System.Globalization.DateTimeStyles.None, out _))
        return "Campo 'fecha' es inválido o está ausente.";
    
    if (!int.TryParse(data[1], out _))
        return "Campo 'mesReporte' es inválido o está ausente.";
    
    if (!int.TryParse(data[2], out _))
        return "Campo 'añoReporte' es inválido o está ausente.";
    
    if (data[3] != "Ingreso" && data[3] != "Egreso")
        return "Campo 'tipoRegistro' es inválido o está ausente.";
    
    if (!decimal.TryParse(data[4], out _))
        return "Campo 'monto' es inválido o está ausente.";

    return null; // Retorna null si todos los campos son válidos
}

private void GenerateSummary(string filePath, bool success, string customMessage = null)
{
    string fileName = Path.GetFileNameWithoutExtension(filePath);
    string summaryPath = Path.Combine(_directoryOutputPath, $"{fileName}_summary.txt");

    using (var writer = new StreamWriter(summaryPath))
    {
        if (customMessage != null)
        {
            writer.WriteLine(customMessage);
        }
        else if (success)
        {
            writer.WriteLine($"File {fileName} processed successfully and data loaded to database.");
        }
        else
        {
            writer.WriteLine($"File {fileName} encountered errors during processing.");
        }
    }

    _logger.LogInformation("Summary file created at {summaryPath}", summaryPath);
}


    private void MoveFileToHistory(string filePath)
    {
        string fileName = Path.GetFileName(filePath);
        string destinationPath = Path.Combine(_directoryHistoryPath, fileName);

        try
        {
            File.Move(filePath, destinationPath); // Mueve el archivo a la carpeta de históricos
            _logger.LogInformation("File {fileName} moved to archive folder successfully.", fileName);
        }
        catch (Exception ex)
        {
            _logger.LogError("Error moving file {fileName} to archive folder: {exception}", fileName, ex.Message);
        }
    }
    

    private string CalculateFileHash(string filePath)
    {
        using (var sha256 = SHA256.Create())
        using (var stream = File.OpenRead(filePath))
        {
            byte[] hashBytes = sha256.ComputeHash(stream);
            return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
        }
    }
    private bool HashExistsInDatabase(string fileHash)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            connection.Open();
            string query = "SELECT COUNT(*) FROM FlujoEfectivo WHERE hash = @hash";
            using (var command = new MySqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@hash", fileHash);
                int count = Convert.ToInt32(command.ExecuteScalar());
                return count > 0;
            }
        }
    }


}
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Ruzzie.Common.IO;

namespace Ruzzie.Azure.Storage
{
    /// <summary>
    /// Can download files from remote blob storage. The downloader only downloads the file if it is newer or not yet exists.
    /// </summary>
    /// <seealso cref="Ruzzie.Common.IO.IFileDownloader" />
    public class AzureBlobFileDownloader : IFileDownloader
    {
        private readonly string _containerName;
        private readonly string _fileName;
        private readonly CloudStorageAccount _cloudStorageAccount;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobFileDownloader"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="fileName">Name of the file.</param>
        /// <exception cref="ArgumentException">
        /// Value cannot be null or whitespace.
        /// or
        /// Value cannot be null or whitespace.
        /// or
        /// Value cannot be null or whitespace.
        /// </exception>
        public AzureBlobFileDownloader(string connectionString, string containerName, string fileName)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(connectionString));
            }
            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(fileName));
            }
            if (string.IsNullOrWhiteSpace(containerName))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(containerName));
            }
            _containerName = containerName;
            _fileName = fileName;

            //Parse the connection string for the storage account.
            _cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
        }

        /// <summary>
        /// Downloads the file if newer or if the file does not yet exists.
        /// </summary>
        /// <param name="localPathToStoreFile">The local path to store file.</param>
        /// <exception cref="ArgumentException">Value cannot be null or whitespace.</exception>
        public void DownloadFile(string localPathToStoreFile)
        {
            if (string.IsNullOrWhiteSpace(localPathToStoreFile))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(localPathToStoreFile));
            }
            CloudBlob blobReference = CreateBlobReferenceForFile();
            blobReference.DownloadToFileAsync(localPathToStoreFile, FileMode.Create).Wait();
        }

        /// <summary>
        /// Downloads the file if newer or if the file does not yet exists.
        /// </summary>
        /// <param name="localPathToStoreFile">The local path to store file.</param>
        /// <exception cref="ArgumentException">Value cannot be null or whitespace.</exception>
        public async Task DownloadFileAsync(string localPathToStoreFile)
        {
            if (string.IsNullOrWhiteSpace(localPathToStoreFile))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(localPathToStoreFile));
            }
            CloudBlob blobReference = CreateBlobReferenceForFile();
            await blobReference.DownloadToFileAsync(localPathToStoreFile, FileMode.Create);
        }

        /// <summary>
        /// Gets the <see cref="IRemoteFileMetaData"/> of the remote file.
        /// </summary>
        /// <value>
        /// The meta data.
        /// </value>
        public IRemoteFileMetaData MetaData
        {
            get
            {
                CloudBlob blobReference = CreateBlobReferenceForFile();
                blobReference.FetchAttributesAsync().Wait();
                return new RemoteFileMetaData {LastModifiedTimeUtc = blobReference.Properties.LastModified?.UtcDateTime};
            }
        }

        private CloudBlob CreateBlobReferenceForFile()
        {
            CloudBlobClient blobClient = _cloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(_containerName);
            CloudBlob blobReference = container.GetBlobReference(_fileName);
            return blobReference;
        }
    }
}
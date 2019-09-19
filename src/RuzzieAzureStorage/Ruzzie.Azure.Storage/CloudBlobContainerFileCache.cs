using System;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Ruzzie.Azure.Storage
{
    public interface IFileCache
    {
        /// <summary>
        /// Gets an item or adds an item if it does not exists.
        /// </summary>
        /// <param name="cacheKey">The cache key.</param>
        /// <param name="directoryName">Name of the directory.</param>
        /// <param name="addFactory">The function that is called if the value is not is cache, it is called with the cacheKey and the directoryName</param>
        /// <returns>The value</returns>
        string GetOrAdd(string cacheKey, string directoryName, Func<string, string, string> addFactory);
    }

    /// <summary>
    /// Cloudblob as a cache.
    /// </summary>
    /// <seealso cref="IFileCache" />
    public class CloudBlobContainerFileCache : IFileCache
    {
        private readonly CloudBlobContainer _container;

        public CloudBlobContainerFileCache(CloudBlobContainer container)
        {
            if (container == null)
            {
                throw new ArgumentNullException(nameof(container));
            }
            _container = container;
        }

        /// <summary>
        /// Gets or add an item.
        /// </summary>
        /// <param name="cacheKey">The cache key.</param>
        /// <param name="directoryName">Name of the directory.</param>
        /// <param name="addFactory">The function that is called if the item is not found in the remote storage, it is called with the cacheKey and the directoryName and the return value will be uploaded as text.</param>
        /// <returns>
        /// The value
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Value cannot be null or whitespace.
        /// or
        /// Value cannot be null or whitespace.
        /// </exception>
        public string GetOrAdd(string cacheKey, string directoryName, Func<string, string, string> addFactory)
        {           
            if (string.IsNullOrWhiteSpace(cacheKey))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(cacheKey));
            }
            if (string.IsNullOrWhiteSpace(directoryName))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(directoryName));
            }
   
            CloudBlobDirectory cloudBlobDirectory = _container.GetDirectoryReference(directoryName);
            CloudBlob blobReference = cloudBlobDirectory.GetBlobReference(cacheKey);
            if (blobReference.ExistsAsync().Result)
            {
                return DownloadAsText(blobReference);
            }

            CloudBlockBlob blockBlobReference = cloudBlobDirectory.GetBlockBlobReference(cacheKey);
            if (!blockBlobReference.ExistsAsync().Result) //double check
            {
                string content = addFactory(cacheKey, directoryName);
                blockBlobReference.UploadTextAsync(content);
                return content;
            }

            return DownloadAsText(blobReference);
        }

        private static string DownloadAsText(CloudBlob blobReference)
        {
            Stream memoryStream = null;
            TextReader reader = null;
            try
            {
                memoryStream = new MemoryStream();
                blobReference.DownloadToStreamAsync(memoryStream).Wait();
                memoryStream.Seek(0, SeekOrigin.Begin);

                reader = new StreamReader(memoryStream, Encoding.UTF8);
                memoryStream = null;//weird stuff to satisfy ca2202

                var result = reader.ReadToEnd();

                reader = null;
                return result;
            }

            finally
            {
                memoryStream?.Dispose();
                reader?.Dispose();
            }
        }
    }
}
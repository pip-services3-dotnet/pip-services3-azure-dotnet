using System;

namespace PipServices3.Azure.Persistence
{
    public class CosmosDbAuthHelper
    {
        public static string GenerateMasterKeyAuthorizationSignature(string verb, string resourceId, string resourceType, string key, string keyType, string tokenVersion, string utcNow)
        {
            var hmacSha256 = new System.Security.Cryptography.HMACSHA256 { Key = Convert.FromBase64String(key) };

            string payLoad = string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}\n{1}\n{2}\n{3}\n{4}\n",
                    verb.ToLowerInvariant(),
                    resourceType.ToLowerInvariant(),
                    resourceId,
                    utcNow.ToLowerInvariant(),
                    ""
            );

            var hashPayLoad = hmacSha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(payLoad));
            var signature = Convert.ToBase64String(hashPayLoad);

            return System.Web.HttpUtility.UrlEncode(string.Format(System.Globalization.CultureInfo.InvariantCulture, "type={0}&ver={1}&sig={2}",
                keyType,
                tokenVersion,
                signature));
        }
    }
}
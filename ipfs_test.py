import ipfshttpclient

client = ipfshttpclient.connect('/ip4/127.0.0.1/tcp/5001/http')

res = client.add('v0_test.txt')
print(res)


"""AES256 암/복호화 샘플
S3에 저장된 오브젝트 중 AES256 암호화된 데이터를 복호화 하기 위한 데이터 검증용 코드...
"""

from base64 import b64decode
from base64 import b64encode

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

def aes_cbc_base64_enc(plain):
    cipher = AES.new(encoded_key, AES.MODE_CBC, iv)
    return bytes.decode(b64encode(cipher.encrypt(pad(plain.encode(),
        AES.block_size))))

def aes_cbc_base64_dec(cipher_text):
    cipher = AES.new(encoded_key, AES.MODE_CBC, iv)
    return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), 
        AES.block_size))

key = '@NcQfTjWnZr4u7x!A%D*G-KaPdSgUkXp'
encoded_key = key.encode()
iv = bytes(16)

data = 'hello world'

encrypted = aes_cbc_base64_enc(data)
decrypted = aes_cbc_base64_dec(encrypted)

print(encrypted)
print(decrypted)
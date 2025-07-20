CREATE USER consumer IDENTIFIED WITH sha256_password BY 'MySecret';
GRANT ALL ON default.* TO consumer;

# HOW TO INSTALL

### If you have any Questions, feel free to contact us on Telegram.
The relayer is designed to connect to exactly one Validator. We recommend installing it on the same machine as the Validator to ensure they share the same IP.

Before installation, make sure the following tools are installed on your machine:
- Rust
- Git
- Solana CLI (or another way to create a keypair)

Next, please install some Dependencies
```bash
sudo apt-get install \
    build-essential \
    pkg-config \
    libudev-dev llvm libclang-dev \
    libssl-dev \
    protobuf-compiler -y
```

Create a folder to store your Keys
```bash
sudo mkdir -p /etc/relayer/keys
```

Create a Solana Keypair - this will be used to verify against our Server
```bash
solana-keygen new --no-bip39-passphrase --outfile relayer-keypair.json
```

Move the keypair to the keys folder
```bash
sudo mv relayer-keypair.json /etc/relayer/keys/
```

Now, create a pem key - Solana is using this for verification
```bash
sudo openssl genrsa --out /etc/relayer/keys/private.pem
```
```bash
sudo openssl rsa --in /etc/relayer/keys/private.pem --pubout --out /etc/relayer/keys/public.pem
```

For the next step, you have to contact us and send us the following information:
- Public key you just created (run `solana address -k /etc/relayer/keys/relayer-keypair.json` if you forgot)
- Public key of your Validator Identity

After that, we'll send you the following information you need in the next steps:
- Proxy Server Address
- Signing String

In the meantime, you can finish the rest of the installation
Navigate into the cloned repository and build the application
```bash
cd os-relayer && cargo build --release
```

Copy the application
```bash
sudo cp target/release/os-relayer /etc/relayer/
```

Move the service file to the systemd folder
```bash
sudo cp os-relayer.service /etc/systemd/system/
```

Edit the service file using
```bash
sudo nano /etc/systemd/system/os-relayer.service
```

Here you have to change the following lines:
- JITO_BLOCKENGINE - fill in the closes Jito Block Engine URL you can find [here](https://docs.jito.wtf/lowlatencytxnsend/#api)
- PROXY - you'll get this from us
- SIGNING_STRING - you'll get this from us

opt. changes you may need:
- WEBSOCKET_SERER - if you don't use the default ip / port at your Validator
- RPC_SERVER - if you don't use the default ip / port at your Validator

Save the file (ctrl + s) and exit (ctrl + x).
Reload the systemd daemon to apply the changes
```bash
sudo systemctl daemon-reload
```

Now, you can start the Service
```bash
sudo systemctl start os-relayer.service
```

You can check if you see any errors using
```bash
sudo journalctl -u os-relayer.service -f
```

If everything works, you can enable the Service to start on boot
```bash
sudo systemctl enable os-relayer.service
```

Lastly, you have to add the following lines to your Startup Script
```
  --block-engine-url "http://127.0.0.1:11225/" \
  --relayer-url http://127.0.0.1:11226/ \
```

You can either restart your Validator or run the following command to apply the changes
```bash
agave-validator --ledger /mnt/ledger/ set-block-engine-config --block-engine-url "http://127.0.0.1:11225"
agave-validator --ledger /mnt/ledger/ set-relayer-config --relayer-url http://127.0.0.1:11226
```

That's it, you should now have a running Relayer!

### Using with SWQOS
If you want to use the Relayer with SWQOS, please add the same overrides flag you're using for the Validator to the Relayer Service file like
```bash
/etc/relayer/os-relayer \
          --keypair-path=/etc/relayer/keys/relayer-keypair.json \
          --signing-key-pem-path=/etc/relayer/keys/private.pem \
          --verifying-key-pem-path=/etc/relayer/keys/public.pem \
          --staked-nodes-overrides=/etc/swqos/overrides.yml
```
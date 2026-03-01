apt-get update
apt-get install -y ffmpeg wget

wget https://github.com/nilaoda/N_m3u8DL-RE/releases/latest/download/N_m3u8DL-RE_linux-x64.tar.gz
tar -xvf N_m3u8DL-RE_linux-x64.tar.gz
chmod +x N_m3u8DL-RE
mv N_m3u8DL-RE /usr/local/bin/
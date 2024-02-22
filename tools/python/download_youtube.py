from pytube import YouTube

path = r'C:\Users\X\Scripts'
filename = 'Troubleshooting com Kubernetes na Cloud | MultiCloud & DevOps'

Link = 'https://www.youtube.com/watch?v=qzv22C2npsk'
yt = YouTube(Link)


print('Fazendo download.....')

video = yt.streams.get_highest_resolution() #para sair em formato mp4
video.download(output_path=path)

print('Download concluido com sucesso!!!')
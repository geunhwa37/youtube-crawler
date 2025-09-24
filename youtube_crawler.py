import os
import json
import re
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import yt_dlp
from faster_whisper import WhisperModel
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# ✅ Secrets
API_KEY = os.environ["YOUTUBE_API_KEY"]
GSHEETS_KEY = os.environ["GSHEETS_KEY"]

# 📌 광고 키워드
ads_keywords = [
    "할인","이벤트","특가","무료검진","보험","비급여","실손",
    "상담","문의","확실","보장","예약","저렴","무료","혜택"
]

# 📌 위험 키워드
risk_keywords = [
    "줄기세포","무릎줄기세포주사","여유증","도수치료","비급여주사",
    "맘모톰","발달지연","요양한방병원","무릎관절증","하지정맥류",
    "갑상선결절","액취증"
]

# 🎙 Whisper 모델 (CPU)
model = WhisperModel("base", device="cpu", compute_type="int8")

# 📌 구글시트 연결 (ID 버전)
def connect_gsheet():
    creds_dict = json.loads(GSHEETS_KEY)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    # 🔑 파일 ID로 열기
    return client.open_by_key("11N-GVX670-a1-pwsA7Qs0o9HwqBgiJOHMgJ7Me-IKjs").worksheet("STT변환결과")


# 📌 데이터 시트 업로드
def upload_to_sheet(df, sheet):
    existing = sheet.get_all_values()
    start_row = len(existing) + 1
    set_with_dataframe(sheet, df, row=start_row, include_column_header=(start_row == 1))
    print(f"✅ 구글시트 업로드 완료 ({len(df)} 행 추가됨)")

# 📌 유튜브 크롤링 (조회수 포함)
def crawl_youtube_videos(keyword, published_after, max_results=20):
    youtube = build("youtube", "v3", developerKey=API_KEY)
    search_response = youtube.search().list(
        q=keyword,
        part="id,snippet",
        type="video",
        order="date",
        publishedAfter=published_after,
        maxResults=max_results
    ).execute()

    videos = []
    for item in search_response.get("items", []):
        video_id = item["id"]["videoId"]

        # 조회수 포함 위해 videos.list 호출
        video_response = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        ).execute()

        if not video_response["items"]:
            continue

        v = video_response["items"][0]
        snippet = v["snippet"]
        stats = v.get("statistics", {})

        videos.append({
            "검색 키워드": keyword,
            "비디오 ID": video_id,
            "제목": snippet["title"],
            "설명": snippet.get("description", ""),
            "채널명": snippet["channelTitle"],
            "올린 날짜": pd.to_datetime(snippet["publishedAt"]).strftime("%Y-%m-%d"),
            "조회수": int(stats.get("viewCount", 0)),
            "URL": f"https://www.youtube.com/watch?v={video_id}",
            "광고성 표현 (T/F)": any(ad in (snippet["title"] + snippet.get("description","")) for ad in ads_keywords)
        })
    return videos


# 📌 STT 변환
def transcribe_video(video_id):
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": f"/tmp/{video_id}.%(ext)s",
        "cookiefile": "cookies.txt",   # ✅ 추가
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
        "quiet": True
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        audio_file = f"/tmp/{video_id}.mp3"
        if not os.path.exists(audio_file):
            return "⚠️ 오디오 없음"
        segments, _ = model.transcribe(audio_file, language="ko")
        text = " ".join([seg.text for seg in segments])
        # 간단 교정
        corrections = {"주기세포":"줄기세포","도수치료법":"도수치료"}
        for wrong, correct in corrections.items():
            text = text.replace(wrong, correct)
        return re.sub(r'\s+', ' ', text).strip()
    except Exception as e:
        return f"⚠️ 오류: {str(e)}"

# 📌 메인
def main():
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    published_after = datetime.combine(yesterday, datetime.min.time()).isoformat("T") + "Z"

    final_data = []
    for kw in risk_keywords:
        videos = crawl_youtube_videos(kw, published_after)
        if not videos: continue
        df = pd.DataFrame(videos)

        # 광고 포함만 필터
        df = df[df.apply(lambda row: any(ad in (row["title"]+row["description"]) for ad in ads_keywords), axis=1)]
        if df.empty: continue

        # STT
        df["transcript"] = df["video_id"].apply(transcribe_video)
        final_data.append(df)

    if final_data:
        df_final = pd.concat(final_data, ignore_index=True)
    
        # 컬럼 순서 고정
        df_final = df_final[
            ["검색 키워드","비디오 ID","제목","설명","채널명",
             "올린 날짜","조회수","URL","광고성 표현 (T/F)"]
        ]
    
        sheet = connect_gsheet()
        upload_to_sheet(df_final, sheet)
    else:
        print("❌ 오늘은 데이터 없음")

if __name__ == "__main__":
    main()





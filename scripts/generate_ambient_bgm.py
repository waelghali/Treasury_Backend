"""
GROW BD — CORPORATE AMBIENT BACKGROUND MUSIC SYNTHESIZER
Generates a warm, polished, executive fintech corporate ambient track (WAV).
"""

import numpy as np
import os
import wave

def generate_ambient_track(output_path, duration_sec=70, sample_rate=44100):
    print(f"Synthesizing corporate ambient background track: {output_path} ({duration_sec}s)")
    total_samples = int(duration_sec * sample_rate)
    t = np.linspace(0, duration_sec, total_samples, endpoint=False)
    
    chords = [
        [146.83, 220.00, 261.63, 329.63, 440.0],
        [116.54, 174.61, 233.08, 293.66, 349.23],
        [174.61, 220.00, 261.63, 329.63, 392.00],
        [130.81, 196.00, 261.63, 293.66, 392.00]
    ]
    
    chord_duration = 8.0
    audio = np.zeros(total_samples, dtype=np.float32)
    
    for i, chord in enumerate(chords * (int(duration_sec // (chord_duration * len(chords))) + 2)):
        c_start = int(i * chord_duration * sample_rate)
        c_end = int((i + 1) * chord_duration * sample_rate)
        if c_start >= total_samples:
            break
        c_end = min(c_end, total_samples)
        c_len = c_end - c_start
        c_t = t[c_start:c_end]
        
        fade_len = int(2.0 * sample_rate)
        env = np.ones(c_len, dtype=np.float32)
        if c_len > 2 * fade_len:
            env[:fade_len] = np.linspace(0, 1, fade_len)
            env[-fade_len:] = np.linspace(1, 0, fade_len)
        else:
            env = np.sin(np.linspace(0, np.pi, c_len))
            
        chord_wave = np.zeros(c_len, dtype=np.float32)
        for freq in chord:
            sine1 = np.sin(2 * np.pi * freq * c_t)
            sine2 = np.sin(2 * np.pi * (freq * 1.003) * c_t)
            chord_wave += (sine1 + sine2) * 0.5
            
        audio[c_start:c_end] += chord_wave * env * 0.18
        
    bass_freqs = [73.42, 58.27, 87.31, 65.41]
    for i, b_freq in enumerate(bass_freqs * 3):
        b_start = int(i * chord_duration * sample_rate)
        b_end = int((i + 1) * chord_duration * sample_rate)
        if b_start >= total_samples:
            break
        b_end = min(b_end, total_samples)
        b_len = b_end - b_start
        b_t = t[b_start:b_end]
        
        env = np.sin(np.linspace(0, np.pi, b_len))
        bass_wave = np.sin(2 * np.pi * b_freq * b_t) + 0.3 * np.sin(2 * np.pi * (b_freq * 2) * b_t)
        audio[b_start:b_end] += bass_wave * env * 0.22
        
    shimmer = np.sin(2 * np.pi * 1760.0 * t) * (0.02 * (0.5 + 0.5 * np.sin(2 * np.pi * 0.25 * t)))
    audio += shimmer
    
    audio = audio / (np.max(np.abs(audio)) + 1e-6) * 0.60
    
    master_fade_in = int(1.5 * sample_rate)
    master_fade_out = int(3.0 * sample_rate)
    audio[:master_fade_in] *= np.linspace(0, 1, master_fade_in)
    audio[-master_fade_out:] *= np.linspace(1, 0, master_fade_out)
    
    audio_int16 = (audio * 32767).astype(np.int16)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with wave.open(output_path, 'wb') as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(audio_int16.tobytes())
            
    print(f"Generated ambient music track successfully: {output_path} ({os.path.getsize(output_path)} bytes)")

if __name__ == "__main__":
    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "audio_assets", "bgm")
    out_file = os.path.join(out_dir, "corporate_ambient_pad.wav")
    generate_ambient_track(out_file)

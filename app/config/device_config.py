DEVICE_CONFIG = {
    "mobile" : {
        "platforms":["ios","android"],
        "network": ["wifi","4g","5g"],
        "pause_reasons":["user_clicked_pause","incoming_call","app_background"],
        "resume_reasons":["user_clicked_play","auto_resume"],
        "abandoned_reasons":["app_closed","network_error"]
    },
    "desktop": {
        "platforms":["web"],
        "network":["wifi"],
        "pause_reasons":["user_clicked_pause","tab_switched"],
        "resume_reasons":["user_clicked_play"],
        "abandoned_reasons":["browser_closed"]
    },
    "tv":{
        "platforms":["webos","tizen"],
        "network":["wifi"],
        "pause_reasons":["remote_pause","idle_timeout"],
        "resume_reasons":["remote_play"],
        "abandoned_reasons":["tv_shutdown"]
    }
}
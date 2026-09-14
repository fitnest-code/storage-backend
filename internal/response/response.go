package response

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var locales = map[string]map[string]string{
	"az": {
		"error.unexpected":            "Gözlənilməz xəta baş verdi. Zəhmət olmasa bir qədər sonra yenidən cəhd edin.",
		"error.internal_server_error": "Texniki xəta baş verdi. Zəhmət olmasa bir qədər sonra yenidən cəhd edin.",
		"error.invalid_json_format":   "Sorğunun formatı düzgün deyil. Zəhmət olmasa göndərilən məlumatı yoxlayın.",
		"error.validation":            "Daxil edilən məlumatlar yanlışdır. Zəhmət olmasa məlumatları yoxlayın.",
		"error.resource_not_found":    "Sorğu edilən məlumat tapılmadı.",
		"error.file_not_found":        "Fayl tapılmadı.",
		"error.access_denied":         "Bu əməliyyat üçün icazəniz yoxdur.",
		"error.bad_request":           "Yanlış sorğu göndərildi.",
		"error.no_file_uploaded":      "Fayl təqdim edilməyib.",
		"error.file_too_large":        "Faylın həcmi 20MB limitini keçir.",
		"error.invalid_file_type":     "Yalnız şəkil fayllarına icazə verilir.",
		"error.upload_failed":         "Fayl yüklənməsi mümkün olmadı.",
		"error.fetch_failed":          "Məlumatların əldə edilməsi mümkün olmadı.",
		"error.delete_failed":         "Silinmə əməliyyatı mümkün olmadı.",
		"error.move_failed":           "Faylın yerinin dəyişdirilməsi mümkün olmadı.",
		"error.stream_failed":         "Sorğu emal edilərkən xəta baş verdi.",
	},
	"en": {
		"error.unexpected":            "An unexpected error occurred. Please try again later.",
		"error.internal_server_error": "A technical error occurred. Please try again later.",
		"error.invalid_json_format":   "Invalid request format. Please check the submitted data.",
		"error.validation":            "The submitted data is invalid. Please review the input.",
		"error.resource_not_found":    "The requested resource was not found.",
		"error.file_not_found":        "File not found.",
		"error.access_denied":         "You do not have permission to perform this operation.",
		"error.bad_request":           "Invalid request.",
		"error.no_file_uploaded":      "No file was provided.",
		"error.file_too_large":        "The file size exceeds the 20MB limit.",
		"error.invalid_file_type":     "Only image files are allowed.",
		"error.upload_failed":         "File upload failed.",
		"error.fetch_failed":          "Failed to retrieve the requested information.",
		"error.delete_failed":         "The delete operation could not be completed.",
		"error.move_failed":           "The file could not be moved.",
		"error.stream_failed":         "An error occurred while processing the request.",
	},
	"ru": {
		"error.unexpected":            "Произошла непредвиденная ошибка. Пожалуйста, попробуйте позже.",
		"error.internal_server_error": "Произошла техническая ошибка. Пожалуйста, попробуйте позже.",
		"error.invalid_json_format":   "Неверный формат запроса. Пожалуйста, проверьте отправленные данные.",
		"error.validation":            "Предоставленные данные недействительны. Пожалуйста, проверьте ввод.",
		"error.resource_not_found":    "Запрошенный ресурс не найден.",
		"error.file_not_found":        "Файл не найден.",
		"error.access_denied":         "У вас нет прав для выполнения этой операции.",
		"error.bad_request":           "Неверный запрос.",
		"error.no_file_uploaded":      "Файл не был предоставлен.",
		"error.file_too_large":        "Размер файла превышает лимит 20 МБ.",
		"error.invalid_file_type":     "Разрешены только изображения.",
		"error.upload_failed":         "Не удалось загрузить файл.",
		"error.fetch_failed":          "Не удалось получить запрошенные данные.",
		"error.delete_failed":         "Не удалось выполнить удаление.",
		"error.move_failed":           "Не удалось переместить файл.",
		"error.stream_failed":         "Произошла ошибка при обработке запроса.",
	},
}

func LoadLocales(dir string) {
	for _, lang := range []string{"az", "en", "ru"} {
		filePath := filepath.Join(dir, lang+".json")
		if data, err := os.ReadFile(filePath); err == nil {
			var m map[string]string
			if err := json.Unmarshal(data, &m); err == nil {
				for k, v := range m {
					locales[lang][k] = v
				}
			}
		}
	}
}

func GetLang(r *http.Request) string {
	al := strings.ToLower(r.Header.Get("Accept-Language"))
	if strings.HasPrefix(al, "az") {
		return "az"
	}
	if strings.HasPrefix(al, "ru") {
		return "ru"
	}
	return "en"
}

func GetMessage(key, lang string) string {
	if m, ok := locales[lang]; ok {
		if val, exists := m[key]; exists {
			return val
		}
	}
	if m, ok := locales["en"]; ok {
		if val, exists := m[key]; exists {
			return val
		}
	}
	return key
}

type ApiResponse struct {
	Data  interface{} `json:"data,omitempty"`
	Error *ApiError   `json:"error,omitempty"`
}

type ApiError struct {
	Code      string      `json:"code"`
	Message   string      `json:"message"`
	Status    int         `json:"status"`
	Path      string      `json:"path"`
	Timestamp string      `json:"timestamp"`
	Details   interface{} `json:"details,omitempty"`
}

func Success(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ApiResponse{Data: data})
}

func Error(w http.ResponseWriter, status int, code, msgKey, path, lang string, details interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ApiResponse{
		Error: &ApiError{
			Code:      code,
			Message:   GetMessage(msgKey, lang),
			Status:    status,
			Path:      path,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Details:   details,
		},
	})
}

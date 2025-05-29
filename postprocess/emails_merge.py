import pandas as pd
import tldextract
import re
from tqdm import tqdm

# === File name settings ===
UNIS_FILE = 'universities.csv'
EMAILS_FILE = 'emails.csv'
OUTPUT_FILE = 'final_emails.csv'
INVALID_FILE = 'invalid_data.csv'

# === Load input files ===
unis_df = pd.read_csv(UNIS_FILE)
emails_df = pd.read_csv(EMAILS_FILE)

# === Extract base domain ===
def extract_base_domain(url):
    try:
        ext = tldextract.extract(str(url))
        return f"{ext.domain}.{ext.suffix}" if ext.domain and ext.suffix else None
    except Exception:
        return None

unis_df['domain'] = unis_df['url'].apply(extract_base_domain)
emails_df['domain'] = emails_df['url'].apply(extract_base_domain)

# === Split emails into separate rows ===
emails_expanded = emails_df.assign(email=emails_df['emails'].str.split(',')).explode('email')
emails_expanded['email'] = emails_expanded['email'].str.strip()

emails_clean = emails_expanded[['domain', 'url', 'email']].rename(columns={'url': 'source_url'})
merged_df = unis_df.merge(emails_clean, on='domain', how='left')

full_df = merged_df[['country_code', 'university_name', 'url', 'source_url', 'email']]
full_df.columns = ['country_code', 'university_name', 'university_url', 'source_url', 'email']

final_df = full_df.copy()

# === Remove by file extensions ===
before_ext = len(final_df)
invalid_ext_mask = final_df['email'].str.lower().str.endswith((
    '.jpg', '.png', '.webp', '.gif', '.jpeg', '.php', '.twig', '.svg', '.pdf',
    '.docx', '.zip', '.asp', '.aspx', '.htm', '.html', '.google.com', '.evil.com',
    '.jsp', '.xml', 'qq.com', '163.com', 'google.com', 'facebookmail.com',
    'amazon.com', 'linkedin.com', 'formsubmit.co', 'sendgrid.net', 'company.com',
    'mailgun.org', 'mandrillapp.com', '.css', 'example.com', 'example.org', '.gmail.com', 'yahoo', 'icloud'
    'domain.com', 'fake-domain.com', 'email.com', 'yourdomain.com', 'yoursite', 'onmicrosoft.com', '.avif'

), na=False)
print(f"🧹 Removed emails with invalid extensions: {invalid_ext_mask.sum()}")

# === Service/system emails ===
service_names = {
    'webmaster', 'web', 'private', 'root', 'hostmaster', 'postmaster', 'phishing',
    'noreply', 'no-reply', 'bounce', 'mailer-daemon', 'support', 'helpdesk', 'help',
    'service', 'security', 'abuse', 'privacy', 'hr', 'do-not-reply', 'donotreply',
    'notifications', 'alerts', 'police', 'username', 'ude.wons', "loans", "loan", 
    "financialaid", "security", 'student', 'students', 'courses', 'course', 'enrollment', 'library',
    'registrar', 'billing', 'enrollments', 'summercamp', 'scholarships', 'scholarship', 'financialaid',
    'career', 'accounts', 'foundation', 'alumni', 'invoice', 'billing', 'studentcareerscentre', 'studentcareers',
    'studentservices', 'studentservicescentre', 'studentservicesdesk', 'studentservicesoffice', 'awards',
    'shop', 'payroll', 'payments', 'payment', 'library', 'grants', 'verification', 'careers', 'safety', 'studentlife', 
    'applications', 'policy', 'copiright', 'copyright', 'enrollment', 'enrollments', 'enrollmentservices', 
    'scholarship', 'scholarships', 'accommodation', 'accommodations', 'housing', 'housingservices', 'student-accommodation',
    'copyright-statement', 'annual-report', 'wellness-centre', 'restaurants-catering-canteens', 'hire-facilities',
    'sponsorships', 'sponsorship', 'fees', 'policy', 'feedback', 'feedbacks', 'feedback-form', 'feedbacks-form',
    'polisci', 'ethics', 'ethics-committee', 'ethics-committee-services', 'ethics-committee-service', 'ethics-committee-management',
    'firstname', 'lastname', 'firstname.lastname', 'firstname_lastname', 'firstname-lastname', 'recruitment', 'campusrecfacilities', 
    'campusstore', 'accreditation', 'campusvisit', 'campus', 'parking', 'sponsored', 'wordpress', 'patents', 'patent', 'patents-office',
    'sysadmin', 'fondation', 'user', 'whoever', 'someone', 'nobody', 'anyone', 'everybody', 'covid', 'covid-19', 'covid19', 'covid19info',
    'netname', 'roombookings', 'is-spam', 'spam', 'spamreport', 'spam-reports', 'spam-reporter', 'spam-reporters', 'print',
    'userid', 'recruitment', 'tours', 'recruitment', 'question', 'questions', 'questionnaire', 'questionnaires', 'questioning', 'questioning-services',
    'sequrite', 'sos', 'recruit', 'gestion-finances', 'pension', 'benefits', 'compensation',  'thesis', 'you', 'example', 'youremail', 'yourname',
    'procurement', 'recruitt', 'events', 'studyabroad', 'sustainability', 'titleix', 'auxiliary-services', 'applicant', 'travel', 'trademarks', 'parent-resources', 'navigators', 'helpdesk', 'conference-services', 'bookcenter', 'campus-ministries', 
    'studentconduct', 'student-conduct', 'student-conduct-services', 'student-conduct-service', 'student-conduct-management', 'ethics-and-compliance',
    'regulatory-compliance', 'support-resources', 'ethical-research-conduct', 'roomscheduling', 'vaccinations', 'compliance', 'studentaccess',
    'bibliotheque', 'bibliotheques', 'biblio', 'restauration-et-alimentation', 'carriere', 'carriere-services', 'securite', 'aide-financiere', 
    'etudiant', 'etudiante', 'etudiants', 'etudiantes', 'employes', 'logistique-et-salles-evenementielles', 'equite-diversite-inclusion', 'services-et-commodites',
    'nos-campus', 'la-boutique', 'boutique', 'evenements', 'aliments-nutrition', 'visite-nous', 'dons', 'donner', 'sante-mentale', 'mental-sante', 
    'bien-etre, bienetre', 'wellness-centre', 'logement', 'logement-etudiants', 'musee', 'museums', 'certificat', 'certificats', 'admission', 'admissions-office', 
    'archives', 'archives-services', 'residences', 'clubs-etudiants', 'soutien', 'support-services', 'inclusion-multiculturelle', 'visite-nous',
    'alimentation', 'etudiantes', 'tudiante', 'donar', 'estudiante', 'estudiantes', 'empleado', 'empleados', 'emplees', 'admisión',  'comida', 'alimentos', 'visitar', 'visita', 
    'familia', 'familias', 'internado', 'internship', 'club', 'clubes', 'voluntario', 'voluntariado', 'documento', 'documentación', 'inscripción', 'enrolamiento', 
    'servicio', 'servicios', 'biblioteca', 'bibliothek', 'sicherheit', 'sicherheitdienste', 'hilfe', 'hilfeleistung', 'veranstaltungen', 'spende', 'spender', 'besuch','besuchen', 
    'arbeiter', 'arbeitgeber', 'verein', 'vereine', 'vereinigen', 'familien', 'praktikum', 'vertraulichkeit', 'studenti', 'studentessa', 'carriera', 'donare', 'famiglia', 'famiglie',
    'visita', 'visitare', 'volontario', 'volontariato', 'donazione', 'donazioni', 'donatore', 'donatori', 'donatrice', 'donatrici', 'donare', 'donare-ora', 'donazione-ora', 
    'impiegati', 'impiegato', 'nutrizione', 'alimentazione', 'doacao', 'estudante', 'estudantes', 'estudante', 'aluno', 'aluna ', 'aluns', 'alums', 'alumnas', 'visite-nos', 'visita',
    'nutricao', 'alimentacao', 'confidencialidade', 'confidencialidade-servicos', 'confidencialidade-servico', 'confidencialidade-gestao', 'confidencialidade-gestao-servicos',
    'seguranca', 'seguridade', 'carreira', 'carreiras', 'evento', 'eventos',

}

def is_service_email(email):
    if pd.isna(email): return False
    username = email.split('@')[0].strip().lower()
    return any(service in username for service in service_names)

service_mask = final_df['email'].apply(is_service_email)
print(f"🚫 Removed service/system emails: {service_mask.sum()}")

# === Likely fake usernames ===
def is_likely_fake_username(email):
    if pd.isna(email): return False
    username = email.split('@')[0].strip()
    return (len(username) >= 25 and username.isalnum()) or username.isdigit()

fake_mask = final_df['email'].apply(is_likely_fake_username)
print(f"🧩 Removed fake usernames (≥25 characters): {fake_mask.sum()}")

# === Clean 'mailto:' and percent-encoding ===
def clean_email(email):
    if pd.isna(email): return email
    email = re.sub(r'%[0-9a-fA-F]{2}', '', email)
    email = email.replace('%', '')
    email = re.sub(r'^u003e', '', email) # ⬅️ delite 'u003e'
    return email.strip()

final_df['email'] = final_df['email'].str.replace(r'^mailto:', '', regex=True)
final_df['email'] = final_df['email'].apply(clean_email)

# === Basic email format validation ===
def is_valid_email_basic(email):
    if pd.isna(email): return False
    parts = email.strip().split('@')
    return len(parts) == 2 and '.' in parts[1]

invalid_email_mask = ~final_df['email'].apply(is_valid_email_basic)
print(f"❓ Removed invalid emails (missing @ or dot): {invalid_email_mask.sum()}")

# === Remove by blacklisted source_url ===
source_url_blacklist_words = {
    "facebook", "linkedin", "sport", "sports", "art", "arts", "sexual", "crime", "phishing", 
    "violence", "football", "police", "human-resources", "financialaid", "loans", 
    "dance", "music", "cosmetology", "security", "privacy", "early-college", "veteran", "veterans", 
    "incident", "incident-report", "incident-reporting", "incident-response", "incident-management",
    "athletics", "housing", "terms-conditions", "terms-and-conditions", "wellnesscenter", "wellness-center", 
    "alumni", "safety", "complaint", "complaints", "store", "shop", "donations", "donation", "GDPR", "GDPR-compliance",
    "support-services", "library", "student-life", "covid", "covid-19", "hire", "hiring", "athlet", "athletic",
    'disability', "disabilities", "disability-services", "disability-support", "disability-accommodations", 'applications', 
    'policy', 'copiright', 'copyright', 'enrollment', 'enrollments', 'enrollmentservices', 
    'scholarship', 'scholarships', 'accommodation', 'accommodations', 'housing', 'housingservices', 'student-accommodation',
    'copyright-statement', 'annual-report', 'wellness-centre', 'restaurants-catering-canteens', 'hire-facilities',
    'sponsorships', 'sponsorship', 'fees', 'payroll', 'payments', 'payment', 'library', 'grants', 'verification', 
    'careers', 'safety', 'studentlife', 'recruitment-process', 'recruitment', 'recruiting', 'recruitment-services',
    'employment', 'graduation', 'annual-reports', 'annual-reporting', 'annual-reporting-services', 'campus-accessibility',
    'volunteer', 'volunteering', 'volunteer-services', 'volunteer-opportunities', 'volunteer-opportunity',
    'volunteer-opportunities', 'volunteer-work', 'volunteer-program', 'volunteer-programs', 'volunteer-programme',
    'volunteer-programmes', 'volunteer-organisation', 'volunteer-organisations', 'volunteer-organization', 'food-services',
    'food-service', 'food-service-management', 'food-service-industry', 'food-service-operations', 'food-service-systems',
    'careers-advisors', 'student-policies-and-guidelines', 'student-policies', 'student-guidelines', 'student-support', 'museum',
    'vacancies', 'vacancy', 'job', 'jobs', 'job-offers', 'job-offer', 'job-vacancies', 'job-vacancy', 'bookstore', 'facilities', 
    'room-bookings', 'room-booking', 'sustainability', 'sustainability', 'sustainable-development', 'enrolment-services', 
    'requirements', 'requirements-services', 'future-students', 'physical-health', 'mental-health', 'subscribe', 'news',
    'funding', 'cunews', 'hospitality', 'hospitality-services', 'alcohol', 'hr', 'telephone-service-basic', 'donner',
    'telephone-service', 'telephone-services', 'telephone-service-providers', 'telephone-service-provider', 'facilities-services',
    'archives', 'archives-services', 'archives-service', 'archives-management', 'archives-management-services', 'career', 'careers',
    'career-services', 'career-service', 'career-management', 'career-management-services', 'career-development',
    'book-stop', 'book-stops', 'book-stop-services', 'book-stop-service', 'book-stop-management', 'student-services', 
    'blogs', 'blog', 'blogs1', 'printing-services', 'events', 'partnerships', 'partnership', 'how-to-make-a-gift', 'gift', 
    'campus-life', 'online-forms', 'online-form', 'online-forms-services', 'online-form-services', 'forms', 'form', 'form-services',
    'pride-fanshawe', 'after-applying', 'part-time-studies', 'part-time-study', 'part-time-study-services', 'part-time-study-service',
    'military-connected-college', 'students-administrative-council', 'archive', 'accessibility', 'accessibility-services', 'accessibility-service',
    'plan-a-visit', 'admission', 'admissions-services', 'admission-services', 'admissions-office', 'admissions-offices',
    'logistics-and-mail', 'logistics-and-mail-services', 'logistics-and-mail-service', 'logistics-and-mail-management',
    'graduate-council', 'health-and-wellness', 'health-and-wellness-services', 'health-and-wellness-service', 'contractors-vendors-and-suppliers',
    'campus-support-services', 'diversity-and-inclusion', 'reports-and-accountability', 'brand', 'foip', 'foip-services', 'foip-service',
    'sustainability-at-the-mount', 'webinar', 'webinars', 'webinar-services', 'registration', 'registration-services', 'registration-service',
    'registrars-office', 'fitness-centre', 'fitness-centres', 'fitness-centre-services', 'fitness-centre-service', 'campus-services', 
    'campus-service', 'student-financial-services', 'student-development-and-services', 'print-plus', 'petitions', 'petitions-services', 'petitions-service',
    'on-campus', 'restaurants-cafes-bakeries-breweries-edmonton-area', 'application-checklist-apprenticeship', 'employer-services', 
    'employer-service', 'employers', 'employer', 'for-journalists', 'health-and-wellbeing', 'media-releases', 'donors', 'contact-hr',
    'foodoptions', 'purchasing', 'marcom', 'wellness', 'new-students', 'roombookings', 'room-bookings', 'room-booking', 'room-booking-services',
    'mycommunity', 'goose-international-youth-camp', 'currentstudents', 'foodservices', 'parking', 'parking-services', 'parking-service', 'parking-management',
    'make-gift', 'nos-campus', 'bibliotheque', 'bibliotheques', 'bibliotheque-services', 'bibliotheque-service', 'bibliotheque-management',
    'biblio', 'employes', 'etudiants', 'etudiante', 'etudiantes', 'etudiant', 'etudiants', 'etudiants-services', 'maps',
    'restauration-et-alimentation', 'restauration-et-alimentation-services', 'restauration-et-alimentation-service', 'disclaimer',
    'services-et-commodites', 'logistique-et-salles-evenementielles', 'carriere', 'carriere-services', 'carriere-service',
    'equite-diversite-inclusion', 'museums', 'calendar', 'locations-and-facilities', 'applying-to-unb', 'welcome-libraries', 
    'academic-support-computing-representatives-group', 'honours-awards', 'about-procurement', 'procurement', 'procurement-services', 'procurement-service',
    'video-conferencing', 'accessibility-statement', 'magazine', 'pride', 'grad-house-restaurant', 'ceremonies', 'gender_equality',
    'dailynews', 'virtual-tours', 'campus-status', 'services-and-spaces', 'news-releases', 'news-release', 'news-releases-services', 'news-release-services',
    'futurestudents', 'agents-list', 'registrar', 'enrol', 'students-council', 'fresh-applicant', 'request-for-trenders', 'campus-facilities',
    'anti-ragging-measures', 'report', 'students-association', 'internship', 'alumbizdirectory', 'enrolment', 'student-experience', 
    'harassment', 'students_union', 'phone-book-student', 'tender', 'student-council', 'studentservices', 'student-services', 'student-service',
    'student-resources', 'student-organizations', 'student-activities', 'testing', 'student-center', 'student-centre', 'student-centers', 'student-centres', 'student-center-services',
    'current-students', 'student-leadership-activities', 'for-students', 'student-concerns', 'student-engagement', 'student-success',
    'civil-rights-compliance', 'student-affairs', 'studentaffairs', 'student-conduct-code', 'student-media', 'resources-students', 
    'financial-aid', 'tuition-and-aid', 'student-right-to-know', 'residence-life', 'residence-life-services', 'residence-life-service', 'residence-life-management',
    'campuslife', 'student-government', 'student-clubs-organizations', 'get-know-campus', 'study-abroad', 'residencelife', 'student_life',
    'religious-life', 'residential-life', 'studentsuccess', 'dining', 'dining-services', 'dining-service', 'dining-management', 'r-kids',
    'student-senate', 'student-involvement', 'money-matters', 'spiritual-life', 'sorority-and-fraternity-life', 'campus-recreation',
    'resident-life', 'securite', 'aide-financiere', 'donate', 'student-affairs-staff', 'student-leadership-opportunities','student-affairs-and-outreach',
    'indigenous-student-affairs', 'weddings', 'collegiate-licensing', 'military', 'student-happiness-center', 'student-wellbeing-center-officers',
    'student-broadcast', 'student_account_office', 'health-center', 'clubs-and-organizations', 'Google-Developer-Student-Club', 'Legislation',
    'student-parliament', 'studentske-sluzbe', 'student-and-cash-section', 'studentcouncil', 'international-students', 'bachelor', 'financial-support',
    'applicants', 'foundation', 'inclusive-communities', 'health-and-well-being', 'ombudsman', 'evenements', 'bookclub', 'certificates',
    'student-parents-centre', 'dean-of-students', 'certificate', 'exam', 'exams', 'antiracism', 'foundations', 'gender-diversity', 'human-rights-advising',
    'web-servers-storage', 'ombudsoffice', 'employee-resources', 'endowment', 'endowment-services', 'endowment-service', 'endowment-management',
    'confidentialite', 'confidentiality', 'confidentiality-services', 'confidentiality-service', 'confidentiality-management', 'who-do-i-call',
    'la-boutique', 'boutique', 'foods-and-nutrition', 'apply', 'grant-support', 'religion-culture', 'residence', 'residences', 'residence-services',
    'conditions-appointment', 'funds', 'cafeteria', 'mail_services', 'campus_life', 'patientcare', 'windowscatering', 'black-history-month', 'multicultural', 'diversity',
    'honors-program', 'honors-programs', 'honors-programme', 'honors-programmes', 'admitted-students', 'PhotoGallery', 'StudentAssistance', 'accreditation', 
    'commercial-drivers-license', 'visit-us', 'visiting-services', 'visiting-service', 'university-transfer', 'parent-promise', 'food-support', 'child-care-center',
    'studentparents', 'legal-affairs', 'press-releases', 'press-release', 'multicultural-identity', 'parents-families', 'children', 'gallery', 'massage-therapy', 
    'transfer', 'transfer-students', 'internprogram', 'campus-engagement', 'clubs-and-activities', 'clubs', 'student-clubs', 'clubs-and-orgs', 
    'studentactivities', 'clubs_and_organizations', 'student-nurse-club', 'conservation-club', 'clubs-organizations', 'student_services_clubs',
    'clubs-orgs', 'parents', 'orthoclub', 'hispanicclub', 'diabetesclub', 'surgclub', 'practiceclub', 'radiologyclub', 'mail-services', 'insurance-billing',
    'student-clubs', 'philosophy-club', 'clubs-honor-societies-ensembles', 'dmc-student-clubs', 'clubs-activities', 'alums', 'food-permit', 'student-groups', 
    'clubs-recreation', 'student_clubs_organizations', 'recruitt', 'vendors', 'vendor', 'vendors-services', 'vendors-service', 'vendors-management',
    'support_services', 'restaurants', 'student-counselling', 'benefits', 'retirees', 'community-and-support', 'work-life-support', 'press-room',
    'ethics-and-compliance-risk-committee', 'chancellor', 'service-support', 'sign-up', 'announcements', 'honors', 'luskinconferencecenter', 'lecturerresources',
    'conflict-interest', 'mailman', 'video', 'funded-projects', 'conferences', 'coronavirus', 'campus-and-community-resources', 'legislation'
    'next-steps', 'families', 'terms-of-use', 'giving', 'services-and-support', 'cutler-center', 'commencement-ceremony', 'marketing-communication',
    'purch', 'public-records', 'engage', 'libraries', 'conference-center', 'neighborhood-relations', 'business-financial-services', '404', 'health-and-counseling-services',
    'advising', 'advising-services', 'advising-service', 'titleix', 'title-ix', 'title-ix-services', 'title-ix-service', 'title-ix-management',
    'ticket-services', 'ticket-service', 'navigate-staff', 'supporting-survivors', 'visitors', 'visitors-services', 'visitors-service', 'visitors-management',
    'auxiliary-services', 'applicant', 'travel', 'trademarks', 'parent-resources', 'navigators', 'helpdesk', 'conference-services', 'bookcenter', 'campus-ministries', 
    'studentconduct', 'student-conduct', 'student-conduct-services', 'student-conduct-service', 'student-conduct-management', 'ethics-and-compliance',
    'regulatory-compliance', 'support-resources', 'ethical-research-conduct', 'roomscheduling', 'vaccinations', 'compliance', 'studentaccess',
    'bibliotheque', 'bibliotheques', 'biblio', 'restauration-et-alimentation', 'carriere', 'carriere-services', 'securite', 'aide-financiere', 
    'etudiant', 'etudiante', 'etudiants', 'etudiantes', 'employes', 'logistique-et-salles-evenementielles', 'equite-diversite-inclusion', 'services-et-commodites',
    'nos-campus', 'la-boutique', 'boutique', 'evenements', 'aliments-nutrition', 'visite-nous', 'dons', 'donner', 'sante-mentale', 'mental-sante', 
    'bien-etre, bienetre', 'wellness-centre', 'logement', 'logement-etudiants', 'musee', 'museums', 'certificat', 'certificats', 'admission', 'admissions-office', 
    'archives', 'archives-services', 'residences', 'clubs-etudiants', 'soutien', 'support-services', 'inclusion-multiculturelle', 'visite-nous',
    'alimentation', 'etudiantes', 'tudiante', 'donar', 'estudiante', 'estudiantes', 'empleado', 'empleados', 'emplees', 'admisión',  'comida', 'alimentos', 'visitar', 'visita', 
    'familia', 'familias', 'internado', 'internship', 'club', 'clubes', 'voluntario', 'voluntariado', 'documento', 'documentación', 'inscripción', 'enrolamiento', 
    'servicio', 'servicios', 'biblioteca', 'bibliothek', 'sicherheit', 'sicherheitdienste', 'hilfe', 'hilfeleistung', 'veranstaltungen', 'spende', 'spender', 'besuch','besuchen', 
    'arbeiter', 'arbeitgeber', 'verein', 'vereine', 'vereinigen', 'familien', 'praktikum', 'vertraulichkeit', 'studenti', 'studentessa', 'carriera', 'donare', 'famiglia', 'famiglie',
    'visita', 'visitare', 'volontario', 'volontariato', 'donazione', 'donazioni', 'donatore', 'donatori', 'donatrice', 'donatrici', 'donare', 'donare-ora', 'donazione-ora', 
    'impiegati', 'impiegato', 'nutrizione', 'alimentazione', 'doacao', 'estudante', 'estudantes', 'estudante', 'aluno', 'aluna ', 'aluns', 'alums', 'alumnas', 'visite-nos', 'visita',
    'nutricao', 'alimentacao', 'confidencialidade', 'confidencialidade-servicos', 'confidencialidade-servico', 'confidencialidade-gestao', 'confidencialidade-gestao-servicos',
    'seguranca', 'seguridade', 'carreira', 'carreiras', 'evento', 'eventos',

}

def is_blacklisted_source_url(url):
    if pd.isna(url):
        return False
    url = url.lower()
    return any(word in url for word in source_url_blacklist_words)

url_blacklist_mask = final_df['source_url'].apply(is_blacklisted_source_url)
print(f"🔗 Removed rows by source_url blacklist: {url_blacklist_mask.sum()}")

# === Combine masks and create invalid_df ===
combined_invalid_mask = invalid_ext_mask | service_mask | fake_mask | invalid_email_mask | url_blacklist_mask
invalid_df = final_df[combined_invalid_mask].copy()
final_df = final_df[~combined_invalid_mask].copy()

# === Convert emails to lowercase ===
final_df['email'] = final_df['email'].str.lower()
final_df['email'] = final_df['email'].str.replace(r'^20', '', regex=True)

# === Calculate removal reasons with progress bar ===
removal_reasons_invalid = []
print("🧠 Generating removal reasons...")
for i in tqdm(invalid_df.index, desc="Removal reasons"):
    reasons = []
    if invalid_ext_mask.loc[i]: reasons.append("extension")
    if service_mask.loc[i]: reasons.append("service_name")
    if fake_mask.loc[i]: reasons.append("fake_username")
    if invalid_email_mask.loc[i]: reasons.append("invalid_format")
    if url_blacklist_mask.loc[i]: reasons.append("source_url")
    removal_reasons_invalid.append(", ".join(reasons))

invalid_df["removal_reason"] = removal_reasons_invalid

# === Remove duplicate emails ===
before_dedup = len(final_df)
final_df = final_df.drop_duplicates(subset='email', keep='first')
after_dedup = len(final_df)
print(f"📈 Before duplicate removal: {before_dedup} строк")
print(f"📉 After duplicate removal: {after_dedup} строк")
print(f"❌ Duplicate emails removed: {before_dedup - after_dedup}")

# === Remove by country codes ===
excluded_country_codes = ['DZ', 'BO', 'GH', 'GT', 'HN', 'DO', 'CU']
before_country = len(final_df)
final_df = final_df[~final_df['country_code'].isin(excluded_country_codes)]
after_country = len(final_df)
print(f"🌍 Before country-based filtering: {before_country}")
print(f"📉 After country-based filtering: {after_country}")
print(f"❌ Rows removed by country filter: {before_country - after_country}")

# === Extra rule: Remove colleges without academic indicators ===
academic_keywords = [
    "faculty", "research", "academics", "department", "program", "school-of", "stem"
]

# === Check for academic indicators in URLs ===
def has_academic_indicator(row):
    urls_to_check = [str(row['source_url']).lower(), str(row['university_url']).lower()]
    return any(keyword in url for url in urls_to_check for keyword in academic_keywords)

final_df['has_academic_url'] = final_df.apply(has_academic_indicator, axis=1)

# Grouping by universities to detect academic indicators
uni_academic_presence = final_df.groupby('university_name')['has_academic_url'].any()

# === List of colleges lacking academic indicators ===
colleges_to_remove = uni_academic_presence[~uni_academic_presence].index

# === Mask for removal by academic rule ===
college_removal_mask = (
    (final_df['country_code'] == 'US') &
    (final_df['university_name'].str.lower().str.contains('community college|technical college')) &
    (final_df['university_name'].isin(colleges_to_remove))
)

# === Remove rows and move to invalid_df ===
removed_college_rows = final_df[college_removal_mask].copy()
removed_college_rows['removal_reason'] = 'community/technical college without academic URLs'

# === Update invalid_df ===
invalid_df = pd.concat([invalid_df, removed_college_rows], ignore_index=True)

# === Remove rows from final_df ===
final_df = final_df[~college_removal_mask].drop(columns=['has_academic_url'])

print(f"🎓 Removed college rows without academic indicators: {college_removal_mask.sum()}")

# === Markup college и white-list ===
white_domains = {
    "pomona.edu", "wellesley.edu", "amherst.edu", "swarthmore.edu",
    "barnard.edu", "harvey.mudd.edu", "colby.edu", "brynmawr.edu",
    "middlebury.edu", "carleton.edu", "davidson.edu", "grinnell.edu",
    "haverford.edu", "macalester.edu", "vassar.edu", "bates.edu",
    "oberlin.edu", "reed.edu", "union.edu", "connecticutcollege.edu",
    "lafayette.edu", "scrippscollege.edu", "rhodes.edu", "whitman.edu",
    "beloit.edu", "trinity.edu", "hamilton.edu", "kenyon.edu", "skidmore.edu"
}

white_names = {
    "Pomona College", "Wellesley College", "Amherst College", "Swarthmore College",
    "Barnard College", "Harvey Mudd College", "Colby College", "Bryn Mawr College",
    "Middlebury College", "Carleton College", "Davidson College", "Grinnell College",
    "Haverford College", "Macalester College", "Vassar College", "Bates College",
    "Oberlin College", "Reed College", "Union College", "Connecticut College",
    "Lafayette College", "Scripps College", "Rhodes College", "Whitman College",
    "Beloit College", "Trinity College", "Hamilton College", "Kenyon College", "Skidmore College"
}

def extract_domain(email):
    try: return email.split("@")[1].lower()
    except: return ""

def check_college_flags(email, university_name):
    domain = extract_domain(email)
    username_flag = "college" in domain
    name_flag = isinstance(university_name, str) and "college" in university_name.lower()
    whitelisted = domain in white_domains or university_name in white_names
    match_reason = "both" if username_flag and name_flag else (
        "email_domain" if username_flag else "university_name" if name_flag else ""
    )
    needs_manual = "yes" if (username_flag or name_flag) and not whitelisted else "no"
    return pd.Series([needs_manual, match_reason, "yes" if whitelisted else "no"])

final_df[["needs_manual_check", "match_reason", "is_whitelisted"]] = final_df.apply(
    lambda row: check_college_flags(row["email"], row["university_name"]), axis=1
)
print(f"🧩 Rows with college tagging (needs_manual_check=yes): {(final_df['needs_manual_check'] == 'yes').sum()}")

# === Save results ===
final_df.to_csv(OUTPUT_FILE, index=False, sep='\t', encoding='utf-8')
invalid_df.to_csv(INVALID_FILE, index=False, sep='\t', encoding='utf-8')
print(f"✅ Done. Saved to file: {OUTPUT_FILE}")
print(f"✅ Invalid rows saved to: {INVALID_FILE}")
print(f"✅ Total emails: {len(final_df)}")
print(f"✅ Total invalid rows: {len(invalid_df)}")
print(f"✅ Total removed rows: {len(combined_invalid_mask)}")
print(f"✅ Total duplicates removed: {before_dedup - after_dedup}")
print(f"✅ Total removed by country: {before_country - after_country}")
print(f"✅ Total removed by extensions: {invalid_ext_mask.sum()}")
print(f"✅ Total service emails removed: {service_mask.sum()}")
print(f"✅ Total fake usernames removed: {fake_mask.sum()}")
print(f"✅ Total invalid emails removed: {invalid_email_mask.sum()}")
print(f"✅ Total removed by source_url blacklist: {url_blacklist_mask.sum()}")
print(f"✅ Total rows removed due to source_url: {url_blacklist_mask.sum()}")

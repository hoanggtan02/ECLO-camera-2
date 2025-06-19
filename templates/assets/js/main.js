// File: main.js
// Chỉ chứa logic giao diện và các sự kiện chung.
// Tương tác với hệ thống real-time thông qua đối tượng `RealtimeService` (được định nghĩa trong file realtime.js)

// ===================================================================
// == KHAI BÁO BIẾN TOÀN CỤC & KHỞI TẠO PJAX ==
// ===================================================================
let active = $('.page-content').attr("data-active");
var pjax = new Pjax({
    elements: "[data-pjax]",
    selectors: [
        "[pjax-load-content]",
        "header",
        "footer",
    ],
    cacheBust: false,
    scrollTo: true,
});

// --- TÍCH HỢP MQTT ---
// Biến toàn cục giữ các instance DataTable đang hoạt động và client MQTT
let activeDataTables = {};
let mqttClient;
// --- KẾT THÚC TÍCH HỢP MQTT ---


// ===================================================================
// == CÁC HÀM HELPER CHO MQTT ==
// ===================================================================
/**
 * Hàm để format dữ liệu thô từ MQTT thành object cho DataTable
 * @param {string} topic Topic của tin nhắn MQTT
 * @param {object} payload Dữ liệu JSON đã được parse
 * @returns {object|null} Object dữ liệu cho DataTable hoặc null
 */
function formatMqttMessageForDataTable(topic, payload) {
    const info = payload.info;
    const eventType = topic.split('/').pop();

    if (eventType === 'Rec') {
        return {
            id:          info.RecordID,
            person_name: info.persionName,
            image_path:  `<img src="${info.pic}" alt="Face" class="img-thumbnail" style="width: 60px;">`,
            event_time:  new Date(info.time).toLocaleString('vi-VN'),
            is_no_mask:  parseInt(info.isNoMask) ? '<span class="badge bg-danger">Không KT</span>' : '<span class="badge bg-success">Có KT</span>',
            person_id:   info.personId,
            similarity:  parseFloat(info.similarity1).toFixed(2) + '%',
            person_type: (parseInt(info.PersonType) == 0 ? 'Nhân viên' : 'Người lạ'),
        };
    } 
    else if (eventType === 'Snap') {
        return {
            id:          info.SnapID,
            person_name: '<b>Người lạ (Snap)</b>',
            image_path:  `<img src="${info.pic}" alt="Face" class="img-thumbnail" style="width: 60px;">`,
            event_time:  new Date(info.time).toLocaleString('vi-VN'),
            is_no_mask:  parseInt(info.isNoMask) ? '<span class="badge bg-danger">Không KT</span>' : '<span class="badge bg-success">Có KT</span>',
            person_id:   '',
            similarity:  '',
            person_type: 'Chụp nhanh',
        };
    }
    return null;
}

/**
 * Hàm cập nhật dữ liệu vào DataTable tương ứng
 * @param {string} topic Topic của tin nhắn MQTT
 * @param {object} data Dữ liệu đã được format để thêm vào bảng
 */
function updateDataTable(topic, data) {
    const eventType = topic.split('/').pop().toLowerCase();

    if (activeDataTables[eventType] && activeDataTables[eventType].instance) {
        const instance = activeDataTables[eventType].instance;
        const newNode = instance.row.add(data).draw(false).node();
        
        $(newNode).addClass('table-success').delay(5000).queue(function(next) {
            $(this).removeClass('table-success');
            next();
        });
    }
}

/**
 * Khởi tạo và kết nối tới MQTT Broker.
 */
function initializeMqttConnection() {
    if (mqttClient && mqttClient.connected) {
        return;
    }
    
    console.log('Đang kết nối tới mqtt.ellm.io...');
    mqttClient = mqtt.connect('wss://mqtt.ellm.io/mqtt', {
        clientId: 'web-client-' + Math.random().toString(16).substr(2, 8),
        username: 'eclo',
        password: 'Eclo@123'
    });

    mqttClient.on('connect', () => {
        console.log('✅ Kết nối MQTT thành công!');
        const topics = ['mqtt/face/1018656/Rec', 'mqtt/face/1018656/Snap'];
        mqttClient.subscribe(topics, (err) => {
            if (err) {
                console.error('Lỗi đăng ký topic:', err);
            } else {
                console.log('Đã đăng ký các topic:', topics.join(', '));
            }
        });
    });

    mqttClient.on('message', (topic, message) => {
        try {
            const payload = JSON.parse(message.toString());
            console.log(`Nhận tin từ [${topic}]:`, payload);
            const formattedData = formatMqttMessageForDataTable(topic, payload);
            if (formattedData) {
                updateDataTable(topic, formattedData);
            }
        } catch (e) {
            console.error('Lỗi xử lý message:', e);
        }
    });

    mqttClient.on('error', (err) => {
        console.error('Lỗi kết nối MQTT:', err);
    });
}


// ===================================================================
// == CÁC HÀM QUẢN LÝ GIAO DIỆN VÀ SỰ KIỆN ==
// ===================================================================

document.addEventListener('pjax:send', pjaxSend);
document.addEventListener('pjax:complete', pjaxComplete);
document.addEventListener('pjax:success', whenDOMReady);
document.addEventListener('pjax:error', pjaxError);

$(document).ready(function() {
    $(document).on("click", "[data-pjax]", function (e) {
        var selector = $(this).data("selector");
        if (selector) {
            var parsedSelectors = selector.split(",").map(function (s) {
                return s.trim();
            });
            pjax.options.selectors = parsedSelectors;
        } else {
            pjax.options.selectors = ["[pjax-load-content]"];
        }
    });
    $(document).on('change', '.checkall', function() {
        var checkbox = $(this).attr('data-checkbox');
        $(checkbox).prop('checked', this.checked);
    });

    themeLayout();
    whenDOMReady();

    // Khởi tạo kết nối real-time một lần duy nhất
    if (typeof initializeMqttConnection !== 'undefined') {
        initializeMqttConnection();
    } else {
        console.error("Lỗi: Hàm initializeMqttConnection is not defined.");
    }
});

function pjaxSend(){
    topbar.show();
    // Xóa các tham chiếu đến DataTable cũ trước khi Pjax tải trang mới
    Object.keys(activeDataTables).forEach(key => delete activeDataTables[key]);
    console.log('Active DataTables đã được dọn dẹp cho Pjax.');
}

function pjaxComplete(){
    topbar.hide();
}

function pjaxError(){
    topbar.hide();
}

function whenDOMReady(){
    datatable();
    dataAction();
    selected();
    upload();
}


// ===================================================================
// == ĐỊNH NGHĨA CÁC HÀM TIỆN ÍCH ==
// ===================================================================

function datatable(){
    $('[data-table]').each(function () {
        const $table = $(this);
        if ($.fn.dataTable.isDataTable($table)) {
            return;
        }
        
        const columns = $table.find('thead th').map(function () {
            const $th = $(this);
            return {
                data: $th.attr('data-name') || null,
                orderable: $th.attr('data-orderable') !== "false",
                visible: $th.attr('data-visible') !== "false",
                className: $th.attr('data-class') || '',
                render: function (data, type, row) {
                    if ($th.attr('data-name') === 'actions') {
                        return $th.attr('data-render');
                    }
                    return data;
                }
            };
        }).get();

        const options = {
            ajax: {
                url: $table.attr('data-url') || null,
                type: $table.attr('data-type') || 'POST',
                data: function(d) {
                    let searchParams = {};
                    return $.extend({}, d, searchParams);
                }
            },
            columns: columns,
            processing: $table.attr('data-processing') === "true",
            serverSide:  $table.attr('data-server') === "true",
            pageLength: parseInt($table.attr('data-page-length')) || 10,
            searching: $table.attr('data-searching') !== "false",
            order: JSON.parse($table.attr('data-order') || '[]'),
            lengthMenu: JSON.parse($table.attr('data-length-menu') || '[[10, 25, 50, 100], ["10", "25", "50", "100"]]'),
            paging: $table.attr('data-paging') !== "false",
            language: JSON.parse($table.attr('data-lang') || '{"search": "","searchPlaceholder": "Nhập để tìm kiếm...","lengthMenu": "_MENU_", "info": "Hiển thị _START_ đến _END_ của tổng _TOTAL_", "infoEmpty":"Hiển thị 0 đến 0 của tổng 0","emptyTable": "Không tìm thấy dữ liệu"}'),
            scrollX: $table.attr('data-scroll-x') || null,
            scrollY: $table.attr('data-scroll-y') || null,
            stateSave: $table.attr('data-state-save') === "true",
            dom: "<'row p-2 align-items-center g-2'<'col-md-6 col-lg-5 col-12 text-start order-2 order-md-1'f><'col-md-6 col-lg-7 col-12 order-1 order-md-2 text-end custom-buttons-display'>>" +
                 "<'row mb-4'<'col-md-12't>>" + 
                 "<'row mb-2 px-2 align-items-center justify-content-between'<'col-md-6 justify-content-start'p><'col-md-6 align-items-center justify-content-md-end d-flex'i l>>",
            initComplete: function () {
                const $buttonSearch = $('.custom-buttons').clone(true);
                $('.custom-buttons-display').html($buttonSearch.html());
            }
        };
        
        var dataTableInstance = $table.DataTable(options);

        const eventType = $table.attr('data-event-type'); 
        if (eventType) {
            activeDataTables[eventType] = { instance: dataTableInstance };
            console.log(`DataTable cho event '${eventType}' đã sẵn sàng nhận dữ liệu real-time.`);
        }
        
        // --- BẮT ĐẦU CODE XỬ LÝ FILTER VÀ DROPDOWN ---
        // Xử lý filter
        $(document).off("click", ".button-filter").on("click", ".button-filter", function() {
            let table = dataTableInstance;
            let filterData = {};
            let params = new URLSearchParams(window.location.search);
            $(".filter-name").each(function() {
                let $el = $(this);
                let name = $el.attr("name");
                let value = $el.val();
                if (value !== "") {
                    filterData[name] = value;
                    params.set(name, value);
                } else {
                    params.delete(name);
                }
            });
            table.settings()[0].ajax.data = function(d) {
                return $.extend({}, d, filterData);
            };
            history.pushState({}, "", "?" + params.toString());
            table.ajax.reload();
        });

        // Reset bộ lọc
        $(document).off("click", ".reset-filter").on("click", ".reset-filter", function() {
            let table = dataTableInstance;
            let params = new URLSearchParams(window.location.search);
            $(".filter-name").each(function() {
                $(this).val("").trigger("change");
                params.delete($(this).attr("name"));
            });
            history.replaceState({}, "", window.location.pathname);
            table.settings()[0].ajax.data = function (d) { return d; };
            table.ajax.reload(null, false);
        });
        // --- KẾT THÚC CODE XỬ LÝ FILTER ---
    });

    // --- BẮT ĐẦU CODE XỬ LÝ DROPDOWN ---
    // Các trình xử lý này áp dụng cho tất cả các bảng có data-table, nên đặt ngoài vòng lặp .each()
    $('[data-table]').off('show.bs.dropdown', '.dropdown').on('show.bs.dropdown', '.dropdown', function () {
        let $dropdownMenu = $(this).find('.dropdown-menu');
        if (!$dropdownMenu.data('original-style')) {
            $dropdownMenu.data('original-style', $dropdownMenu.attr('style') || '');
        }
        $('body').append($dropdownMenu.detach());
        let newStyle = `${$dropdownMenu.data('original-style')}; display: block; position: absolute; top: ${$(this).offset().top + $(this).outerHeight()}px; left: ${$(this).offset().left}px;`;
        $dropdownMenu.attr('style', newStyle);
        $(this).data('dropdown-menu', $dropdownMenu);
    });

    $('[data-table]').off('hidden.bs.dropdown', '.dropdown').on('hidden.bs.dropdown', '.dropdown', function () {
        let $dropdownMenu = $(this).data('dropdown-menu');
        if ($dropdownMenu) {
            $(this).append($dropdownMenu.detach());
            $dropdownMenu.attr('style', $dropdownMenu.data('original-style'));
            $(this).removeData('dropdown-menu');
        }
    });
    // --- KẾT THÚC CODE XỬ LÝ DROPDOWN ---
}

function themeLayout() {
    function setTheme(theme) {
        if (theme === 'system') {
            theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
        }
        $("body").attr("data-bs-theme", theme);
    }
    function toggleSidebar() {
        const currentLayout = $("body").attr("data-sidebar");
        const newLayout = currentLayout === 'full' ? 'small' : 'full';
        $("body").attr("data-sidebar", newLayout);
        localStorage.setItem('layout', newLayout);
    }
    let theme = localStorage.getItem('theme') || 'system';
    localStorage.setItem('theme', theme);
    setTheme(theme);
    if (theme === 'system') {
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (event) => {
            setTheme('system');
        });
    }
    $(document).on("click", '[data-toggle-theme]', function () {
        const newTheme = $(this).attr("data-theme");
        localStorage.setItem('theme', newTheme);
        setTheme(newTheme);
    });
    $(document).on("click", '[data-toggle-sidebar]', toggleSidebar);
    const savedLayout = localStorage.getItem('layout') || 'full';
    $("body").attr("data-sidebar", savedLayout);
    if (!getCookie('did')) {
        setCookie('did', generateUUID(), 365);
    }
}

function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0,
            v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function setCookie(name, value, days) {
    var expires = "";
    if (days) {
        var date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        expires = "; expires=" + date.toUTCString();
    }
    document.cookie = name + "=" + (value || "") + expires + "; path=/";
}

function getCookie(name) {
    var nameEQ = name + "=";
    var ca = document.cookie.split(';');
    for (var i = 0; i < ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) == ' ') c = c.substring(1, c.length);
        if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
    }
    return null;
}

function dataAction(){
    $('[data-action="load"]').each(function () {
        var $this = $(this);
        var $url = $this.attr('data-url');
        if ($url) {
            $.ajax({
                type: 'POST',
                dataType: 'json',
                url: $url,
                success: function(response) {
                    if (response && response.content) {
                        $this.find(".spinner-load").remove();
                        $this.html(response.content);
                        pjax.refresh();
                    }
                },
                error: function(xhr, ajaxOptions, thrownError) {
                    console.error('Lỗi tải data-action="load":', thrownError); 
                }
            });
        } else {
            $this.removeAttr('disabled');
        }
    });

    $(document).off('click submit', '[data-action]').on('click submit', '[data-action]', function (event) {
        let $this = $(this);
        let type = $this.attr('data-action');
        if (type === 'blur' || type === 'change') return;

        event.preventDefault();
        event.stopImmediatePropagation();
        $this.attr('disabled', 'disabled');

        let $url = $this.attr('data-url');
        let form = $this.closest('form');
        let checkbox = $this.attr('data-checkbox');
        
        if(checkbox){
            var checkedItems = $(checkbox + ":checked").map(function() {
                return $(this).val();
            }).get();

            if (checkedItems.length > 0) {
                $url = $url + '?list=' + checkedItems.join(',');
            } else {
                swal_error('Vui lòng chọn dữ liệu');
                $this.removeAttr('disabled');
                return;
            }
        }
        
        let options = {
            alert: $this.attr("data-alert"),
            load: $this.attr("data-load"),
            form: $this.attr("data-form"),
            multi: $this.attr("data-multi"),
            remove: $this.attr("data-remove"),
        };

        let formData = new FormData();
        let dsPOST = false;

        switch (type) {
            case 'submit':
                dsPOST = true;
                $url = $url || (form.length ? form.attr('action') : '');
                formData = new FormData(form[0]);
                break;
            case 'click':
                dsPOST = true;
                if (options.form) {
                    handleFormData(formData, options.form, $this);
                }
                if ($url && $url.includes('?list=')) {
                    let params = new URLSearchParams($url.split('?')[1]);
                    formData.append('list', params.get('list'));
                    $url = $url.split('?')[0];
                }
                break;
            case 'modal':
            case 'offcanvas':
                handleModalOrOffcanvas(type, $url, options.multi, $this);
                return;
        }
        
        if ($url && dsPOST) {
            sendAjaxRequest($url, formData, options, $this);
        } else {
            $this.removeAttr('disabled');
        }
    });
}

function swal_success(text,$this=null) {
    Swal.fire({
        title: 'Thành công',
        text: text,
        icon: 'success',
        showCancelButton: false,
        buttonsStyling: false,
        confirmButtonText: 'Ok',
        customClass: {
            confirmButton: "btn fw-bold btn-success rounded-pill px-5"
        }
    }).then(function(isConfirm) {
        if (isConfirm && $this) {
            $('.modal-load').modal('hide');
            $('.modal-views').remove();
        }
    });
}

function swal_error(text) {
    Swal.fire({
        title: 'Lỗi!',
        text: text,
        icon: 'error',
        showCancelButton: false,
        buttonsStyling: false,
        confirmButtonText: 'Ok',
        customClass: {
            confirmButton: "btn fw-bold btn-danger rounded-pill px-5"
        }
    });
}

function selected(){
    if ($.fn.selectpicker) {
        $('[data-select]').selectpicker();
    }
}

function upload(){
    let dropArea = $("#drop-area");
    let fileInput = $("#file-input");
    let uploadBtn = $("[data-upload]");
    let url = uploadBtn.attr("data-url");
    let fileListContainer = $("#file-list");
    let selectedFiles = [];
    let uploadedFiles = new Set();
    dropArea.on("dragover", function (e) {
        e.preventDefault();
        $(this).addClass("bg-light");
    });
    dropArea.on("dragleave", function (e) {
        e.preventDefault();
        $(this).removeClass("bg-light");
    });
    dropArea.on("drop", async function (e) {
        e.preventDefault();
        $(this).removeClass("bg-light");
        let items = e.originalEvent.dataTransfer.items;
        let files = e.originalEvent.dataTransfer.files;
        if (items && items.length > 0) {
            let hasFolder = false;
            let promises = [];
            for (let item of items) {
                let entry = item.webkitGetAsEntry();
                if (entry) {
                    if (entry.isDirectory) hasFolder = true;
                    promises.push(traverseFileTree(entry, ""));
                }
            }
            await Promise.all(promises);
            if (!hasFolder && files.length > 0) {
                handleFiles(files);
            }
        } else if (files.length > 0) {
            handleFiles(files);
        }
    });
    dropArea.on("click", function () {
        fileInput.click();
    });
    fileInput.on("change", function () {
        handleFiles(this.files);
    });
    async function processDataTransferItems(items) {
        for (let item of items) {
            let entry = item.webkitGetAsEntry();
            if (entry) {
                await traverseFileTree(entry, "");
            }
        }
        if (selectedFiles.length > 0) {
            uploadBtn.show();
        }
    }
    async function traverseFileTree(item, path) {
        return new Promise((resolve) => {
            if (item.isFile) {
                item.file((file) => {
                    let relativePath = path + file.name;
                    if (!uploadedFiles.has(relativePath)) { // Kiểm tra file đã upload chưa
                        selectedFiles.push({ file, relativePath });
                        displayFile(file, relativePath);
                    }
                    resolve();
                });
            } else if (item.isDirectory) {
                let dirReader = item.createReader();
                let newPath = path + item.name + "/";
                dirReader.readEntries(async (entries) => {
                    if (entries.length === 0) {
                        selectedFiles.push({ file: null, relativePath: newPath }); // Đánh dấu thư mục rỗng
                        displayFile(null, newPath);
                    }
                    for (let entry of entries) {
                        await traverseFileTree(entry, newPath);
                    }
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }
    function handleFiles(files) {
        Array.from(files).forEach(file => {
            if (!uploadedFiles.has(file.name) && !selectedFiles.some(f => f.file?.name === file.name)) {
                selectedFiles.push({ file, relativePath: file.name });
                displayFile(file, file.name);
            }
        });

        if (selectedFiles.length > 0) {
            uploadBtn.show();
        }
    }
    function getFileIcon(file) {
        let fileType = file.type.toLowerCase();
        let fileName = file.name.toLowerCase();

        if (fileType.startsWith("image/")) return URL.createObjectURL(file);
        if (fileType === "application/pdf") return "templates/assets/icons/pdf.png";
        if (fileType.includes("text")) return "templates/assets/icons/files.png";
        if (fileType.includes("rar")) return "templates/assets/icons/rar.png";
        if (fileType.includes("zip")) return "templates/assets/icons/zip.png";
        if (fileType.includes("audio/")) return "templates/assets/icons/audio.png";

        // Kiểm tra tất cả định dạng PowerPoint
        if (
            fileName.endsWith(".ppt") ||
            fileName.endsWith(".pptx") ||
            fileName.endsWith(".pps") ||
            fileName.endsWith(".ppsx")
        ) {
            return "templates/assets/icons/ppt.png";
        }

        // Kiểm tra tất cả định dạng Word
        if (
            fileName.endsWith(".doc") ||
            fileName.endsWith(".docx") ||
            fileName.endsWith(".dot") ||
            fileName.endsWith(".dotx") ||
            fileName.endsWith(".rtf")
        ) {
            return "templates/assets/icons/doc.png";
        }

        // Kiểm tra tất cả định dạng Excel
        if (
            fileName.endsWith(".xls") ||
            fileName.endsWith(".xlsx") ||
            fileName.endsWith(".xlsm") ||
            fileName.endsWith(".csv")
        ) {
            return "templates/assets/icons/xls.png";
        }

        // Mặc định là files.png nếu không thuộc các loại trên
        return "templates/assets/icons/files.png";
    }
    function displayFile(file, displayPath) {
        let fileItem = $("<div>").addClass("file-item border position-relative p-2 rounded-4 w-100 mb-2");

        let fileHtml = `<div class="d-flex justify-content-between align-items-center position-relative z-2">
            <div class="d-flex align-items-center w-75 col-12 text-truncate">
                ${file ? `<img src="${getFileIcon(file)}" class="width me-2" style="--width:30px;">` : '<i class="ti ti-folder"></i>'}
                <div class="col-12 text-truncate"><span>${displayPath}</span><span class="text-danger small file-error d-block"></span></div>
            </div>
            <div class="file-action">
                <button class="removeItem btn p-0 border-0"><i class="ti ti-trash fs-4 text-danger"></i></button>
            </div>
        </div>`;

        fileItem.append(fileHtml);
        fileListContainer.append(fileItem);

        fileItem.find(".removeItem").on("click", function (e) {
            e.stopPropagation();
            selectedFiles = selectedFiles.filter(f => f.relativePath !== displayPath);
            fileItem.remove();
            if (selectedFiles.length === 0) {
                uploadBtn.hide();
            }
        });
    }
    uploadBtn.on("click", function () {
        if (selectedFiles.length === 0) return;
        uploadBtn.prop("disabled", true);
        let newFilesToUpload = selectedFiles.filter(f => !uploadedFiles.has(f.relativePath));

        if (newFilesToUpload.length === 0) {
            uploadBtn.prop("disabled", false);
            return;
        }
        uploadFiles(selectedFiles.indexOf(newFilesToUpload[0]));
    });
    function uploadFiles(index) {
        if (index >= selectedFiles.length) {
            uploadBtn.prop("disabled", false);
            let load = uploadBtn.attr("data-load");
            pjax.loadUrl(load === 'this' ? '' : load);
            return;
        }
        let { file, relativePath } = selectedFiles[index];
        if (uploadedFiles.has(relativePath)) {
            uploadFiles(index + 1);
            return;
        }
        let formData = new FormData();
        formData.append("path", relativePath); 
        if (file) {
            formData.append("file", file);
        }
        let progressBar = $("<div>").addClass("progress position-absolute bg-body top-0 start-0 w-100 h-100 rounded-4")
            .append($("<div>").addClass("progress-bar bg-primary bg-opacity-10 progress-bar-striped progress-bar-animated"));
        fileListContainer.children().eq(index).append(progressBar);
        progressBar.show();
        fileListContainer.children().eq(index).find(".removeItem").hide();
        $.ajax({
            url: url,
            type: "POST",
            data: formData,
            contentType: false,
            processData: false,
            xhr: function () {
                let xhr = new window.XMLHttpRequest();
                xhr.upload.addEventListener("progress", function (e) {
                    if (e.lengthComputable) {
                        let percent = Math.round((e.loaded / e.total) * 100);
                        progressBar.children(".progress-bar").css("width", percent + "%");
                        let progressText = fileListContainer.children().eq(index).find(".file-action .progress-text");
                        if (progressText.length) {
                            progressText.text(percent + "%");
                        } else {
                            fileListContainer.children().eq(index).find(".file-action").append('<span class="fs-6 fw-bold text-primary progress-text">' + percent + '%</span>');
                        }
                    }
                }, false);
                return xhr;
            },

            success: function (response) {
                if (response.status === 'success') {
                    progressBar.children(".progress-bar").removeClass("bg-primary").addClass("bg-success");
                    uploadedFiles.add(relativePath);
                    fileListContainer.children().eq(index).find(".removeItem").remove();
                    fileListContainer.children().eq(index).find(".file-action .progress-text").remove();
                    fileListContainer.children().eq(index).find(".file-action").append('<i class="ti ti-circle-check fs-2 text-success"></i>');
                }
                else {
                    progressBar.children(".progress-bar").removeClass("bg-primary").addClass("bg-danger");
                    fileListContainer.children().eq(index).find(".removeItem").show();
                fileListContainer.children().eq(index).find(".file-error").text(response.content);
                    fileListContainer.children().eq(index).find(".file-action .progress-text").remove();
                    fileListContainer.children().eq(index).find(".file-action .removeItem").html('<i class="ti ti-xbox-x fs-2 text-danger"></i>');
                }
                uploadFiles(index + 1);
                
            },
            error: function () {
                progressBar.children(".progress-bar").css("width", "100%");
                progressBar.children(".progress-bar").removeClass("bg-primary").addClass("bg-danger");
                fileListContainer.children().eq(index).find(".removeItem").show();
                fileListContainer.children().eq(index).find(".file-error").text('Error Connection');
                fileListContainer.children().eq(index).find(".file-action .progress-text").remove();
                fileListContainer.children().eq(index).find(".file-action .removeItem").html('<i class="ti ti-xbox-x fs-2 text-danger"></i>');
                uploadFiles(index + 1);
            }
        });
    }
}

function handleFormData(formData, dataFormString, $this) {
    try {
        let parsedData = JSON.parse(dataFormString);
        for (let key in parsedData) {
            let value = parsedData[key];
            let selector = value.replace(/\.(val|html|text)$/, '');
            let method = value.match(/\.(val|html|text)$/)?.[1];
            formData.append(key, method ? $(selector === 'this' ? $this : selector)[method]() : selector);
        }
    } catch (error) {
        console.error("Error parsing data-form:", error);
    }
}

function handleModalOrOffcanvas(type, $url, multi, $this) {
    $.ajax({
        type: 'GET',
        url: $url,
        dataType: 'json',
        success: function(response) {
            if (response && response.status === 'error') {
                swal_error(response.content);
                $this.removeAttr('disabled');
                return;
            }
            if (response && response.content) {
                const modalContent = $(response.content);
                const viewClass = multi ? `${type}-view-${multi}` : `${type}-views`;

                if (!multi) {
                    $(`.${type}-views`).remove();
                }
                $('body').append(modalContent);

                const $target = modalContent.is(`.${type}`) ? modalContent : modalContent.find(`.${type}`);
                
                if (type === 'modal') {
                    $target.modal('show');
                } else if (type === 'offcanvas') {
                    $target.offcanvas('show');
                }

                $target.on(`hidden.bs.${type}`, function () {
                    $(this).closest(`.${viewClass}`).remove();
                });
            } else {
                 console.error("Response không hợp lệ hoặc thiếu content.");
            }
             $this.removeAttr('disabled');
        },
        error: function(xhr) {
            console.error('Lỗi AJAX:', xhr.responseText);
            swal_error('Lỗi tải nội dung từ server.');
            $this.removeAttr('disabled');
        }
    });
}

function sendAjaxRequest(url, formData, options, $this) {
    $.ajax({
        type: 'POST',
        url: url,
        data: formData,
        dataType: 'json',
        cache: false,
        contentType: false,
        processData: false,
        success: function (response) {
            if (response.status === 'error') {
                swal_error(response.content);
            } else if (response.status === 'success') {
                if (options.alert) {
                    swal_success(response.content, $this);
                }
                if (options.remove) {
                    $(options.remove).remove();
                }
                if (options.load) {
                    if (options.load === 'this') {
                        pjax.reload();
                    } else {
                        pjax.loadUrl(options.load);
                    }
                }
            }
            $this.removeAttr('disabled');
        },
        error: function (xhr) {
            console.error('Lỗi AJAX:', xhr.responseText);
            swal_error('Đã xảy ra lỗi không xác định khi gửi dữ liệu.');
            $this.removeAttr('disabled');
        }
    });
}

if ('serviceWorker' in navigator && 'PushManager' in window) {
    navigator.serviceWorker.register('/sw.js')
    .then(function(registration) {
        // console.log('Service Worker đã được đăng ký:', registration);
    })
    .catch(function(error) {
        // console.error('Lỗi đăng ký Service Worker:', error);
    });
}
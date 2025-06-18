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
document.addEventListener('pjax:send', pjaxSend);
document.addEventListener('pjax:complete', pjaxComplete);
document.addEventListener('pjax:success', whenDOMReady);
document.addEventListener('pjax:error',pjaxError);
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
});
function pjaxSend(){
  topbar.show();
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
  // new DataTable('#example');
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
              url: $url,
              success: function(response) {
                var getData = response;
                  $this.find(".spinner-load").remove();
                  $this.html(getData.content);
                  // whenDOMReady();
                  pjax.refresh();
              },
              error: function(xhr, ajaxOptions, thrownError) {
                  console.error('Error:', thrownError); 
              }
          });
      } else {
          $this.removeAttr('disabled'); // Remove 'disabled' attribute if no URL
      }
  });
  $(document).off('click submit', '[data-action]').on('click submit', '[data-action]', function (event) {
      let $this = $(this);
      let type = $this.attr('data-action');
      let $url = $this.attr('data-url');
      let form = $this.closest('form');
      let checkbox = $this.attr('data-checkbox');
      if (type === 'blur' || type === 'change') return; // Loại bỏ blur trong sự kiện click
      $this.attr('disabled', 'disabled');
      if (!$this.is(':checkbox')) {
        event.preventDefault();
        event.stopImmediatePropagation();
      }
      if(checkbox){
        if ($(checkbox+":checked").length > 0) {    
            var boxid = [];
            $(checkbox+':checkbox:checked').each(function(i){
                boxid[i] = $(this).val();
            });
          $url = $url+'?box='+boxid;
        }
        else {
            swal_error('Vui lòng chọn dữ liệu');
            topbar.hide();
            $this.removeAttr('disabled');
            return;
        }
      }
      let options = {
          alert: $this.attr("data-alert"),
          load: $this.attr("data-load"),
          form: $this.attr("data-form"),
          multi: $this.attr("data-multi"),
          socket: $this.attr("data-socket"),
          socketCode: $this.attr("data-socket-code") || 'send',
          stream: $this.attr("data-stream") || 'false',
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
              break;
          case 'social':
              dsPOST = true;
              handleSocialData(formData, $this);
              break;
          case 'modal':
          case 'offcanvas':
              handleModalOrOffcanvas(type, $url, options.multi, $this);
              return;
      }
      if (options.socket && dsPOST) {
          sendSocketData(formData, options);
      }
      if ($url && dsPOST) {
          sendAjaxRequest($url, formData, options, $this);
      }
  });
  $('[data-action="blur"]').on('blur', function () {
      let $this = $(this);
      let $url = $this.attr('data-url');
      if (!$url) return;
      let formData = new FormData();
      let formAttr = $this.attr("data-form");
      let options = {
          alert: $this.attr("data-alert"),
          load: $this.attr("data-load"),
          form: $this.attr("data-form"),
          multi: $this.attr("data-multi"),
          socket: $this.attr("data-socket"),
          socketCode: $this.attr("data-socket-code") || 'send',
          stream: $this.attr("data-stream") || 'false',
          remove: $this.attr("data-remove"),
      };
      if (formAttr) {
          handleFormData(formData, formAttr, $this);
      }
      sendAjaxRequest($url, formData, options, $this);
  });
}
function swal_success(text,$this=null) {
  Swal.fire({
    title: 'Success',
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
    title: 'Error!',
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
function datatable(){
  $('[data-table]').each(function () {
    const $table = $(this);
    const columns = $table.find('thead th').map(function () {
        const $th = $(this);
        return {
            data: $th.attr('data-name') || null,
            orderable: $th.attr('data-orderable') === "true",
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
    const searchableColumns = columns
        .map((col, index) => (col.visible ? index : null))
        .filter(index => index !== null);
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
        searching: $table.attr('data-searching') === "true",
        order: JSON.parse($table.attr('data-order') || '[]'),
        lengthMenu: JSON.parse($table.attr('data-length-menu') || '[[10, 25, 50, 100, 200 , 500], ["10", "25", "50", "100", "200", "500"]]'),
        paging: $table.attr('data-paging') !== "false",
        language: JSON.parse($table.attr('data-lang') || '{"search": "","searchPlaceholder": "Nhập để tìm kiếm...","lengthMenu": "_MENU_", "info": "Hiển thị _START_ đến _END_ của tổng _TOTAL_", "infoEmpty":"Hiển thị 0 đến 0 của tổng 0","emptyTable": "Không tìm thấy dữ liệu"}'),
        scrollX: $table.attr('data-scroll-x') || null,
        scrollY: $table.attr('data-scroll-y') || null,
        stateSave: $table.attr('data-state-save') || null,
        dom: "<'row p-2 align-items-center g-2'<'col-md-6 col-lg-5 col-12 text-start order-2 order-md-1'f><'col-md-6 col-lg-7 col-12 order-1 order-md-2 text-end custom-buttons-display'>>" +
             "<'row mb-4'<'col-md-12't>>" + 
             "<'row mb-2 px-2 align-items-center justify-content-between'<'col-md-6 justify-content-start'p><'col-md-6 align-items-center justify-content-md-end d-flex'i l>>",
        button: [],
        initComplete: function () {
            const $buttonSearch = $('.custom-buttons').clone(true);
            $('.custom-buttons-display').html($buttonSearch.html());
        }
    };
    if (!$.fn.dataTable.isDataTable($table)) {
        var dataTableInstance = $table.DataTable(options);
    } else {
        var dataTableInstance = $table.DataTable();
    }
    $(document).off("click", ".button-filter").on("click", ".button-filter", function() {
        let table = dataTableInstance;
        let filterData = {};
        let params = new URLSearchParams(window.location.search);
        $(".filter-name").each(function() {
            let $el = $(this);
            let name = $el.attr("name");
            let value = "";

            if ($el.is("select") || $el.is("input")) {
                value = $el.val();
            } else if ($el.is("input[type='checkbox'], input[type='radio']")) {
                if ($el.is(":checked")) {
                    value = $el.val();
                }
            }
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
        table.settings()[0].ajax.data = function (d) {
            return d; // Không truyền filterData nữa
        };
        table.ajax.reload(null, false);
    });
    $(document).ready(function() {
        let params = new URLSearchParams(window.location.search);
        let filterData = {};
        $(".filter-name").each(function() {
            let $el = $(this);
            let name = $el.attr("name");
            if (params.has(name)) {
                let value = params.get(name);
                $el.val(value).trigger("change");
                filterData[name] = value;
            }
        });
        if (Object.keys(filterData).length > 0) {
            dataTableInstance.settings()[0].ajax.data = function(d) {
                return $.extend({}, d, filterData);
            };
            dataTableInstance.ajax.reload();
        }
    });
  });
  $('[data-table]').on('show.bs.dropdown', '.dropdown', function () {
        let $dropdownMenu = $(this).find('.dropdown-menu');
        if (!$dropdownMenu.data('original-style')) {
            $dropdownMenu.data('original-style', $dropdownMenu.attr('style') || '');
        }
        $('body').append($dropdownMenu.detach());
        let newStyle = `${$dropdownMenu.data('original-style')}; display: block; position: absolute; top: ${$(this).offset().top + $(this).outerHeight()}px; left: ${$(this).offset().left}px;`;

        $dropdownMenu.attr('style', newStyle);
        $(this).data('dropdown-menu', $dropdownMenu);
    });

    $('[data-table]').on('hidden.bs.dropdown', '.dropdown', function () {
        let $dropdownMenu = $(this).data('dropdown-menu');
        if ($dropdownMenu) {
            $(this).append($dropdownMenu.detach());
            $dropdownMenu.attr('style', $dropdownMenu.data('original-style'));
            $(this).removeData('dropdown-menu');
        }
    });
}
function selected(){
  $(function () {
      $('[data-select]').selectpicker();
  });
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
function handleSocialData(formData, $this) {
    let parent = $this.parents(".social-create");
    formData.append('content', parent.find(".social-post").html());
    formData.append('type', parent.find("input[name='type']").val());
    formData.append('access', parent.find("input[name='access']:checked").val());

    let mediaType = formData.get('type');
    if (mediaType === 'audio') {
        formData.append('voice', parent.find("input[name='voice']").val());
    } else if (mediaType === 'video') {
        formData.append('video', parent.find("input[name='video']").val());
    } else if (mediaType === 'images') {
        parent.find("input[name='images[]']").each(function () {
            let imageUrl = $(this).val();
            if (imageUrl) formData.append('images[]', imageUrl);
        });
    }
}
function handleModalOrOffcanvas(type, $url, multi, $this) {
    let viewClass = `${type}-view${multi ? '-' + multi : '-views'}`;
    if (!$(`.${viewClass}`).length) $('<div>').addClass(viewClass).appendTo('body');

    if (!$url) return;

    $(`.${viewClass}`).load($url, function (response, status) {
        if (status === "error") {
            $(`.${viewClass}`).remove();
            swal_error('ERROR');
        } else {
            let $target = $(`.${viewClass} .${type}-load`);
            $target[type]('show').on(`shown.bs.${type}`, () => topbar.hide());
            selected();
            upload();
            $('[data-pjax]').on("click", function () {
                $(`.${viewClass} .${type}-load`).removeClass(type);
                $(`.${viewClass}`).remove();
            });
        }
    }).on(`hidden.bs.${type}`, function () {
        $(`.${viewClass}`).remove();
        $this.removeAttr('disabled');
    });
}
function sendSocketData(formData, options) {
    let jsonObject = Object.fromEntries(formData.entries());
    let socketPayload = {
        status: "success",
        sender: active,
        token: getToken,
        stream: options.stream,
        router: options.socket,
        data: jsonObject,
        code: options.socketCode
    };
    send(socketPayload);
}
function sendAjaxRequest(url, formData, options, $this) {
    $.ajax({
        type: 'POST',
        url: url,
        data: formData,
        cache: false,
        contentType: false,
        processData: false,
        success: function (response) {
            topbar.hide();
            if (response.status === 'error') {
                swal_error(response.content);
            } else if (response.status === 'success') {
                if (options.remove) $(options.remove).remove();

                if (options.load) {
                    if (response.load === 'true') {
                        window.location.href = options.load;
                    } else if (response.load === 'url') {
                        window.location.href = response.url;
                    } else if (response.load === 'ajax') {
                        pjax.loadUrl(response.url);
                    } else {
                        pjax.loadUrl(options.load === 'this' ? '' : options.load);
                    }
                }

                if (options.alert) {
                    swal_success(response.content, $this);
                }
            }
            $this.removeAttr('disabled');
        },
        error: function () {
            topbar.hide();
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
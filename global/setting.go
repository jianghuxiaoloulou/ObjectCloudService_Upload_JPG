package global

import (
	"WowjoyProject/ObjectCloudService_Upload_JPG/pkg/logger"
	"WowjoyProject/ObjectCloudService_Upload_JPG/pkg/setting"
)

var (
	ServerSetting   *setting.ServerSettingS
	GeneralSetting  *setting.GeneralSettingS
	DatabaseSetting *setting.DatabaseSettingS
	ObjectSetting   *setting.ObjectSettingS
	Logger          *logger.Logger
)

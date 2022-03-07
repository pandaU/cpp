#include "Reflection.h"
#include <beFdeGeometry/Point.h>
#include "../WorkSpace/Public.h"
#include "../MainWindow/MainForm.h"
#include "../MainWindow/SetDeviationDialog.h"

Reflection::Reflection(QWidget *parent)
{
	ui.setupUi(this);
	/*ui.zoy_page->setChecked(true);
	ui.copy_page->setChecked(true);*/
	//this->setWindowIcon(QIcon(":/qrc/mmlogo.ico"));
	/*ui.copy_page->setChecked(true);*/
	connect(ui.zoy_page,SIGNAL(clicked()),this,SLOT(Select_radioButton()));
	connect(ui.zox_page, SIGNAL(clicked()), this, SLOT(Select_radioButton()));
	connect(ui.xoy_page, SIGNAL(clicked()), this, SLOT(Select_radioButton()));
	connect(ui.copy_page, SIGNAL(clicked()), this, SLOT(Select_radioButton()));
	connect(ui.cannotcopy_page, SIGNAL(clicked()), this, SLOT(Select_radioButton()));
	connect(ui.OK, SIGNAL(clicked(bool)), this, SLOT(slotOK()));
	connect(ui.cancel, SIGNAL(clicked(bool)), this, SLOT(slotCancel()));
	connect(this, &Reflection::signalTransStop, &MainForm::createInstance(), &MainForm::slotTransStop, Qt::ConnectionType::QueuedConnection);
	connect(this, &Reflection::signalSendTempDrawData, MainForm::createInstance().getProjectionFinalView(), &projectionFinalView::slotGetDrawData);
	connect(this, &Reflection::signalSendTempGuidLines, MainForm::createInstance().getProjectionFinalView(), &projectionFinalView::slotGetGuideLineData);

	connect(this, &Reflection::signalEditFinish, MainForm::createInstance().getProjectionFinalView(), &projectionFinalView::slotDoneCreateGraphics);
};
Reflection::~Reflection()
{
};
//void Reflection::slotGetRadioGroupStatus()
//{
//}
void Reflection::slotOK()
{
	if (!MainForm::createInstance().getBasePoint().valid())
	{
		return;
	}
	SModel& _Model = ReconstructCom::createInstance().getCurrentModel();
    _point = MainForm::createInstance().getBasePoint()->getPosition();
	if (model.drawGroups.size() > 0)
	{
		for (int k = 0; k < model.drawGroups.size(); k++)
			model.drawGroups[k].deleteRenderOutline();
	}
	model.clearAllDrawGroup();
	reflection(_point, _normal, _Model, model);
	if (!_copy_or_not)
	{
		for (int j = eraseDrawgroup.size() - 1; j>-1; j--)
		{
			SDrawGroup& dg = _Model.drawGroups[eraseDrawgroup[j]];
			_Model.eraseGroup(dg);
		}
	}
	for (int k = 0; k < model.drawGroups.size(); k++)
	{
		_Model.pushGroup(model.drawGroups[k]);
		/*SDrawGroup sdg = model.drawGroups[k];
		_Model.pushGroup(sdg);*/
		std::vector<EditInfo> editInfo;
		editInfo.push_back(EditInfo(k, editUpdate));
		emit signalEditFinish(editInfo);
	}
	for (int k = 0; k < _Model.drawGroups.size(); k++)
	{
		_Model.drawGroups[k].creatRenderModel();
	}
	
	ReconstructCom::createInstance().saveTempModel();
	this->close();
};

void Reflection::Select_radioButton()
{
	if (ui.zoy_page->isChecked())
	{
		if (!MainForm::createInstance().getBasePoint().valid())
		{
			return;
		}
		_point = MainForm::createInstance().getBasePoint()->getPosition();
		SModel& _Model = ReconstructCom::createInstance().getCurrentModel();
		if (model.drawGroups.size() > 0)
		{
			for (int k = 0; k < model.drawGroups.size(); k++)
				model.drawGroups[k].deleteRenderOutline();
		}
		model.clearAllDrawGroup();
		_normal = MainForm::createInstance().getBasePlaneXAxis();
		int num = _Model.drawGroups.size();
		getReflectionMatrix(_point, _normal, _matix);
		reflection(_point, _normal, _Model, model);
		for (int k = 0; k < num; k++)
		{
			_Model.drawGroups[k].creatRenderModel();
		}
		for (int k =0; k < model.drawGroups.size(); k++)
		{
			model.drawGroups[k].createRenderOutline();
		}
		ReconstructCom::createInstance().saveTempModel();
	}
	if (ui.zox_page->isChecked())
	{
		if (!MainForm::createInstance().getBasePoint().valid())
		{
			return;
		}
		_point = MainForm::createInstance().getBasePoint()->getPosition();
		SModel& _Model = ReconstructCom::createInstance().getCurrentModel();
		if (model.drawGroups.size() > 0)
		{
			for (int k = 0; k < model.drawGroups.size(); k++)
				model.drawGroups[k].deleteRenderOutline();
		}
		model.clearAllDrawGroup();
		_normal = -MainForm::createInstance().getBasePlaneNormal() ^ MainForm::createInstance().getBasePlaneXAxis();
		int num = _Model.drawGroups.size();
		getReflectionMatrix(_point, _normal, _matix);
		reflection(_point, _normal, _Model,model);
		for (int k = 0; k < num; k++)
		{
			_Model.drawGroups[k].creatRenderModel();
		}
		for (int k = 0; k < model.drawGroups.size(); k++)
		{
			model.drawGroups[k].createRenderOutline();
		}
		ReconstructCom::createInstance().saveTempModel();
	}
	if (ui.xoy_page->isChecked())
	{
		if (!MainForm::createInstance().getBasePoint().valid())
		{
			return;
		}
		_point = MainForm::createInstance().getBasePoint()->getPosition();
		SModel& _Model = ReconstructCom::createInstance().getCurrentModel();
		if (model.drawGroups.size() > 0)
		{
			for (int k = 0; k < model.drawGroups.size(); k++)
				model.drawGroups[k].deleteRenderOutline();
		}
		model.clearAllDrawGroup();
		_normal = MainForm::createInstance().getBasePlaneNormal();
		int num = _Model.drawGroups.size();
		getReflectionMatrix(_point, _normal, _matix);
		reflection(_point, _normal, _Model, model);
		for (int k = 0; k < num; k++)
		{
			_Model.drawGroups[k].creatRenderModel();
		}
		for (int k = 0; k < model.drawGroups.size(); k++)
		{
			model.drawGroups[k].createRenderOutline();
		}
		ReconstructCom::createInstance().saveTempModel();
	}
	if (ui.cannotcopy_page->isChecked())
	{
		_copy_or_not = false;
	}
	if (ui.copy_page->isChecked())
	{
		_copy_or_not = true;
	}
}
void Reflection::slotCancel()
{
	SModel& _Model = ReconstructCom::createInstance().getCurrentModel();
	if (model.drawGroups.size() > 0)
	{
		for (int k = 0; k < model.drawGroups.size(); k++)
			model.drawGroups[k].deleteRenderOutline();
	}
	this->close();
};

void Reflection::reflection(beMath::Vec3f point, beMath::Vec3f normal, SModel& Model,SModel&showOut)
{
	SModel& _Model = ReconstructCom::createInstance().getCurrentModel();
	int num = Model.drawGroups.size();
	getReflectionMatrix(point, normal, _matix);
	
	for (int k = 0; k < num; k++)
	{
		SDrawGroup& sdg = Model.drawGroups[k];
		for (int i = 0; i < sdg.planeVec.size(); i++)
		{
			SPlane& plane = sdg.planeVec[i];
			if (plane.isSelected)
			{
				eraseDrawgroup.push_back(k);
				break;
			}
		}
	}
	if (eraseDrawgroup.size() == 0)
	{
		QMessageBox::information(this, "MirrorTool", QString::fromLocal8Bit("选择集为空"), QString::fromLocal8Bit("确定"));
	}
	for(int k=0;k< num;k++)
	{
		std::vector<SPlane> face;
		
		SDrawGroup& sdg = Model.drawGroups[k];
		SDrawGroup sdgRef;
		sdgRef.normalVec = sdg.normalVec;
		sdgRef.textureCoordVec = sdg.textureCoordVec;
		for (int k = 0; k < sdg.vertexVec.size(); k++)
		{
			beMath::Vec4d data = beMath::Vec4d(sdg.vertexVec[k].x(),
				sdg.vertexVec[k].y(),
				sdg.vertexVec[k].z(),
				1);
			beMath::Vec4d ref4ddata = _matix.preMult(data);
			beMath::Vec3d ref3ddata = beMath::Vec3d(ref4ddata.x(), ref4ddata.y(), ref4ddata.z());
			sdgRef.pushVertex(ref3ddata);
		}
		for (int i = 0; i < sdg.planeVec.size(); i++)
		{
			SPlane& plane = sdg.planeVec[i]; 
			SPlane planeref;
			if (plane.isSelected)
			{
				planeref.texureName = sdg.planeVec[i].texureName;
				for (int s = 0; s < plane.vectorAttrs.size(); s++)
				{
					/*beMath::Vec3f dsv=sdg.getVertex(plane.vectorAttrs[s].indexVertex);
					beMath::Vec4d data = beMath::Vec4d(dsv.x(),
						dsv.y(),
						dsv.z(),
						1);
					beMath::Vec4d ref4ddata = _matix.preMult(data);
					beMath::Vec3d ref3ddata = beMath::Vec3d(ref4ddata.x(), ref4ddata.y(), ref4ddata.z());
					sdgRef.pushVertex(ref3ddata);*/
					SVectorAttribute davec;
					davec.indexVertex = plane.vectorAttrs[plane.vectorAttrs.size() - s - 1].indexVertex;
					davec.indexNormal = plane.vectorAttrs[plane.vectorAttrs.size() - s - 1].indexNormal;
					davec.indexTextureCoord = plane.vectorAttrs[plane.vectorAttrs.size() - s - 1].indexTextureCoord;
					planeref.vectorAttrs.push_back(davec);
				}
				sdgRef.pushPlane(planeref);
			}
		}
		showOut.pushGroup(sdgRef);
		//Model.pushGroup(sdgRef);
	}
	/*if (_copy_or_not)
	{

	}
	else
	{
		for (int j = eraseDrawgroup.size()-1; j>-1; j--)
		{
			SDrawGroup& dg = Model.drawGroups[eraseDrawgroup[j]];
			Model.eraseGroup(dg);
		}
	}*/
}
void Reflection::getReflectionMatrix(beMath::Vec3f point, beMath::Vec3f normal, beMath::Matrixd& matix)
{
	normal.normalize();
	float d = - normal * point;
	/*std::cout << d << "d" << std::endl;*/
	matix = beMath::Matrixd(1-2* normal.x()*normal.x(), -2* normal.x()*normal.y(), -2 * normal.x()*normal.z(),0,
		-2* normal.x()*normal.y(), 1 - 2 * normal.y()*normal.y(), -2 * normal.y()*normal.z(),0,
		-2 * normal.x()*normal.z(), -2 * normal.y()*normal.z(), 1 - 2 * normal.z()*normal.z(),0,
		-2 * (d*normal.x()), -2 * (d*normal.y()), -2 * (d*normal.z()), 1);
}









